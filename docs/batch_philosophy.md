# batch CC の整理

2020-12-14

## この文書の目的

- shirakami 上で長時間にわたるトランザクション処理 (long tx) を、短期間の多量のトランザクション処理 (short tx) と共存させるための方策を整理する

## 前提

- long tx は short tx の操作によって abort されない

## 方針

以下のいずれか

- (A) long tx のトランザクションオーダーを、当該 long tx の終了時点に設定する
- (B) long tx のトランザクションオーダーを、当該 long tx の開始時点に設定する
- (C) not serializable - long tx を含む history を serializable にしない

### (A) long tx のトランザクションオーダーを、当該 long tx の終了時点に設定する

- 考えられる制約
  - long tx のすべての read step が対象とするページに対し、 long tx と衝突する並行 short tx は long tx の終了まで (blind write を除く) write step を行えない。
    - long tx の read step は前置するすべての short tx の write step の結果を反映しなければならない
    - 並行する short tx であっても、 commit 済みであれば write step の結果を long tx は読んでよい (この short tx は long tx と衝突しておらず、前置できるため)。
  - 複数の long tx を並行して稼働させようとすると非常に複雑になる
    - 前置する long tx の write step と、後置する long tx の read step が同時に存在してはならない。それを許容する場合、read-other-write によって循環 (clown anormaly) が発生しかねないので、ケアする必要がある。
    - long tx が途切れずカスケードして存在する場合、 short tx の置き所がなくなる
- 制約の緩和
  - short tx が blind write step のみであれば、blind write の後置にヒストリを作成する流れで short tx を後置できるため write step を行える. workload に多量の blind write が欲しい。
- 備考
  - 広範囲に read lock 相当が必要なため、 read intensive なバッチによってオンライン処理性能が劣化する
  - short tx が read lock によってその操作を妨げられるため、性能が劣化する
  - 複数の long tx を並行稼働させるのは実質的に無理？**アボートさせない**ロングトランザクションは悲観的に実行し、ステップごとに確定させたい。ステップごとに確定させるプロトコル上において、複数の実行単位が存在する場合、実行単位同士で read other write 循環が発生しうるため、**アボートさせない** ことが厳しい。これはロングトランザクションを高速化のために複数の mini tx に分割しつつ、全ての mini tx が同一ビューを見るときも同様である。

### (B) long tx のトランザクションオーダーを、当該 long tx の開始時点に設定する

- 考えられる制約
  - 基本的にオンライン処理は当該 long tx よりも後置することを考える。
  - long tx のすべての write step が対象とするページに対し、 short tx は long tx の終了まで read step を行えない (排他制御が必要)
    - short tx の read step は前置するすべての long tx の write step の結果を反映しなければならない
    - read & report. テーブル集合Aに対して、オンライン処理とバッチ処理は読み込み操作を行いうる。バッチ処理は書き込まない。テーブル集合Bに対してバッチ処理は読み込まない、書き込みうる。オンライン処理は read only tx でのみ読み込みうる。下記テーブルにそれらをまとめる。オンライン処理が table set B に対して read only tx を実行するときは、バッチ処理に対して前置し、古いスナップショットを読む。Table set A に対して書き込むとき、バッチ処理が後で読み込みに来れるようにスナップショットに古いバージョンを退避させる。このようにすると、オンライン処理はバッチの書き込みを待つことは基本的になくなり、バッチに読み込まれていても（それは前置されることが制約として確定しているため）すぐさま上書きすることができる。
  - オンライン処理とバッチ処理で書き込み先を分けられない場合、オンライン処理はバッチがいつどこに書き込むか分からないため、(blind write を除いて)書き込みをすぐに確定できない。blind write 頼みとなると、それは (A) 案と近似する。

    | | オンライン処理 | バッチ処理 |
    |:-:|:-:|:-:|
    |Table set A | tx read, tx write, read only tx | tx read, read only tx |
    |Table set B | read only tx | write |


- 備考
  - long tx の書き込み範囲があらかじめ判明していなければならない。換言すると、バッチが書き込む先に同時並行的にオンライン処理が読まないことが判明していなければいけない。ナイーブにはテーブルを分ける。
  - read, write 等の性能劣化は最低限に抑えられるが、処理内容に制約がある。
  - 複数の long tx を書き込み先が互いに素であれば並行稼働させられる。

### (C) not serializable

アイデアレベル
- A 案 + enable parallel batch + allow clown anomaly between batches   
  clown anomaly がバッチ間において発生すること（batch を構成するオペレーションが線形拡張できないこと）を許容するのであれば、並列にバッチを走らせられるし、一つのバッチを複数の mini tx に分割して走らせることもできる。
- B 案 + オンライン処理とバッチ間の write 対象の排他制御なし  
  バッチの書く値をオンライン処理は信用できなくても（参考にするだけで）良い場合、オンライン処理はバッチの書き込み先を読み、バッチが終わるのを待たずに tx read operation を終える。

## Future work

### batch 対策を行ったプロトコルにおける epoch-framework と read only optimization

- read only transaction はどれだけ古い view を見ることが許容できるか？
- epoch-framework から serialization order を外しても良いか？例えばバッチトランザクションはステップごとに確定させるとして、下記テーブルのような実行スケジュールがあったとき、serialization order は batch -> short tx と取れるが、 epoch を serialization order の最優先要素とするとき、 short tx -> batch と order を取ることになるために矛盾が生じる。

|time|short tx|batch tx|
|:-:|:-:|:-:|
|epoch 1| | w(x1) |
| | r(x1) | |
|epoch 2| w(y1) | |
| | commit | |
|epoch 3| | w(z1) |
| | | commit |
