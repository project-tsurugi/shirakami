# Philosophy
執筆開始 : 2020-11-17 tanabe

## この文書について
shirakami の設計思想とアルゴリズム構成と，その強みとなるワークロード・弱みとなるワークロードをまとめる．

## 前提知識
本セクションには次セクションに用いる用語解説等を行う．
- 楽観的技術，悲観的技術<br>
共有データに対する排他技術は大きく分けて，悲観的技術と楽観的技術がある．
悲観的な技術は読み込み操作においてもロック機能を担うデータへの更新を伴う．
楽観的な技術は書き込み操作によって更新されるロックの機能を担う単調増加ユニークな値をアトミックに読む操作でデータボディ読み込み操作を挟み，
前後の読み込んだ値が不変であることによりデータボディアトミック読み込みを提供する．


## Shirakami の設計思想
　shirakami は楽観的技術をベースとしたトランザクショナルシステムである．Tsurugi project として新たな DBMS を創出するにあたって，それとの
比較のために現代アーキテクチャ上においてシンプルかつ広範なワークロードで高い性能 (pre-commit/s) を発揮するようにモダンな技術を組み合わせて
設計された．
現代アーキテクチャとしてはメニーコア，大容量 DRAM を想定しており，persistent memory, non-volatile memory 等は想定していない．
本システムは主に索引構造 (In-memory tree indexing) ・並行性制御・ロギング技術 (checkpointing) から構成される．


楽観的技術を中心に採用した理由は，悲観的技術に比べてキャッシュに親和的だからである．
現代アーキテクチャ上において，高い計算性能を発揮するためには一般的にキャッシュに親和的である方が良いと考えられる．
悲観的技術は楽観的技術よりも多くのデータ更新を伴い，それは近い未来に利用予定のキャッシュにあるデータをメモリに追い出してしまったり，
空間的に集中的 (ex. high skew access) な操作を受けたときに現代の複数ソケットアーキテクチャではキャッシュ一貫性プロトコル稼働による
コストを多大に支払うことになる．
楽観的技術はそれを避けるためのものである．
将来的にデータベースの単位時間あたりにおけるトランザクション処理量が増加の一途をたどることが期待されるとして，トランザクションを直列化するために
用いられる悲観的技術によって発生しうるキャッシュ一貫性プロトコル稼働によるコストは深刻な性能劣化要因につながるため，避けたい．
楽観的技術のデメリットとして，アトミックであるとみなしたい操作から，検証操作までの時間間隔が広く空いてしまったとき，割り込まれることによって
楽観検証に失敗するリスクがある．
これは多数のオペレーションを含むトランザクショナル操作において顕著に表れる．
従って，楽観的技術はロングトランザクションを得意なワークロードとしていない．
本システムにおいては，ロングトランザクションを除いてベストなスコアを目指したものである．
そのため，ワークロードは highly selective であるとし，楽観的な In-memory tree indexing アーキテクチャと並行性制御を採用した．

 ロギング技術は主に WAL 形式とチェックポインティング形式が検討され，非同期チェックポインティング技術を中心に研究開発を進める予定である．
 重要視する 性能要素は pre-commit/s とし，コミットレイテンシの増大は許容する方針である．
 近年，古くからある WAL 形式は高性能化のために並列化・epoch-base 化が進められてきた．
 これはアーキテクチャの主流が HDD から SSD に, マルチコアからメニーコアに，シングルソケットから多ソケットへ変化してきたことによって下記のよう
 な性能要件に変化してきたことがあげられる．
  - 性能ボトルネックが IOPS よりも centralized (logging)に傾いてきた &rarr; parallel logging.
  - 単一 log sequence number の操作が多数ソケット(メニーコアアーキテクチャ)においてキャッシュ一貫性プロトコルによる深刻な性能劣化を引き起こす,
   一度の書き込み命令における書き込み量は多いほど良い &rarr; epoch-base (高遅延)<br>
   
 ここで，WAL 形式においても pre-commit/s を稼ぐには高遅延を許容せざるを得ないことが分かる．
 トランザクション処理において並行性制御コストが一定と仮定したとき，pre-commit/s を稼ぐにはログ永続化関係の計算量が 0 に近いことが理想である．
 そこで，ワーカースレッドがログ永続化関係の計算をなるべく行わなくて済むような近年提案された非同期チェックポインティング技術を中心に研究開発を進めている．

## Shirakami のターゲットビルド構成
- Overall : low contention, short transaction 向けのアーキテクチャである．
- 索引構造 : yakushima (based on Masstree)
  - Strong points : concurrent put/get/delete, low (r/w) contention workloads, epoch based parallel resource management. <br >
    put によって引き起こされる競合ポイントはボーダーノード．多数のレコード（挿入）によって多数のボーダーノードが存在する場合，競合しにくい．
    write (put/delete) でしかロックを取らず，読み込み処理は楽観的にアトミック操作．
    従って high concurrency である. 
    単一セッション内で読み込んだ値が並行して削除され，再度その読み込んだ値にアクセスしてメモリアクセス違反を起こさないように，
    セッションという枠組みでそれを保護するとともに，削除処理によって発生した garbage node を並行して解放可能な仕組みがある．
  - Weak points : scan, high (r/w) contention workloads. <br>
  タイムスタンプ（バージョンフィールド）の不変によってアトミック read 操作を保証している．
  この楽観的技術は low contention workloads において（悲観技法と比較してロッキングによってキャッシュを汚さないために）高い読み込み性能を発揮する．
  read はタイムスタンプ不変のタイミングを高速につかみ取りたいが，put/delete 過多なワークロードではその楽観検証に失敗するリスクが増加する．
  また，scan operation はボーダーノードのファンアウトサイズ分 (key-)value をアトミックに読み込む必要があるが，ロックの粒度はノードであるため，
  read 同様に書き込み過多なワークロードでは楽観検証に失敗するリスクが増加する．
  - 想定議論
    - high concurrency ? : yes. <br>
    ロックの粒度が小さい．
    ボーダーノードのファンアウトを小さくすれば一定レコード数に対してより多くのボーダーノード（競合ポイント）となり，並行性は上がるが，
    この場合ノード分割に伴いメモリ確保(ヒープ競合），メモリ使用量増加等も伴う．
    そういったことのうまくバランスが取れた（とオリジナル論文著者らが定めた）設定がファンアウト 16.
- 並行性制御 : based on Silo
  - Strong points : low (r/w) contention workloads, short transaction, no centralized structure (without epoch), 
  read only snap(?), optimistic technique, epoch based parallel resource management. <br>
  read は楽観的技術を用いるため，上記同様低競合環境にて高い性能を発揮する．
  索引構造 yakushima はアトミック操作であるが，Silo はトランザクショナル操作であることと，楽観的技術が用いられていることから，
  ショートトランザクションで高い性能を発揮する．
  順序調停に関するメタデータテーブルを持たず，中央構造は eopch カウンタのみであり，これの変更は（論文著者らの設定では） 40 ms とキャッシュ
  汚染影響を受けないようにすることができる．従ってキャッシュに親和的である．エポック単位でリソース管理をバッチ処理する．
  - Weak points : high (r/w) contention workloads), long transaction(ex. scan). <br>
  楽観的技術を用いるため，高競合環境に不利である．
  また，transaction life time が伸びることは楽観的技術に不利に働くため，ロングトランザクションも苦手である．
- ロギング : based on Concurrent Prefix Recovery
  - Strong points : high throughput (against wal), holding 1 copy, parallel creating copy(?), silo's read only snap 統合
  の可能性. <br>
  wal と比較して，（チェックポインティングフェーズに入らなければ）ロギングにまつわる処理が何もないため high throughput (pre-commit/s) で
  ある．
  Zigzag, Ping-Pong と比べて，保有するコピー数が 1 と少なくて省メモリである．
  チェックポイントフェーズにおいて，チェックポインターが到達する前のレコードにアクセスしたワーカーはコピー作成を肩代わりすることができる．
  これはチェックポイント時間短縮と pre-commit/s 低下の影響があり，何を優先するかで肩代わりするしないは（設計あるいは将来的なビルドオプション
  として）選択可能である．
  CPR copy と silo's read only snap shot は統合可能な期待が強い．
  - Weak points : latency due to snapshot approach. <br>
  チェックポイントを作成し終えるまで durable にならず，そのレイテンシは小さくない．
  差分チェックポイント技術を適応してチェックポイント作成時間を短縮した場合，差分チェックポイントをいかにマージするか，マージのために何らかの
  ロジックの quiescent time はあるか，それでもレイテンシが総合的に短縮となるか，という検討が必要となる．