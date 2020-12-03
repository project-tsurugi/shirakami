# ロング（バッチ）トランザクションのための機構
執筆開始 : 2020/12/01 田辺

## long read transaction への対策
todo
- Philosophy : todo

## long read write transaction への対策
バッチ処理（ロングトランザクション）を確実にコミットさせながら，オンライン処理（ショートトランザクション）の高い性能を発揮する需要がある
(ex. パン屋さんの原価計算処理）． 本セクションでは read / write が混在するロングトランザクションを想定し，執筆する．
- Philosophy : serialization order において, long tx の過去に飛ばすか未来に飛ばす．
Other tx が long tx をまたいだらアボートする．
以下non-deterministic を想定する（deterministic であれば，より柔軟に構築する）. 

- long tx は read phase で読むレコードに対して事前に predicate lock (for preventing from phantom), ロックA をかける．
Predicate にひっかかる insert は phantom 問題を起こさないために，long tx が終わるまで許容しない．
ロック A のかかったレコードには other tx は long tx’s read が validation phase で read verify に失敗しないようにするため write 
できない(未来を待つ)．
Other tx がロック A を読んで no lock なレコードに書くのは許容する (read other write)．
その場合，serialization order としては，並置か過去となる．

- ロックA の解放待ちをしている other tx’s write が多い場合，ロックA をロック B にアップグレードし，直後のステップとなる other tx’s read 
を許容せず，blind write だけを許容することもできる. 
換言すると blind write のトランザクションを serialization order 上の未来に置く．
Read が許容できない状況は batch’s write が来るかもしれなくて，read はそれを読まないといけないからである．
Blind write を許容した場合，そのトランザクション A はバッチトランザクションよりも未来の順序となるため，直後から並行トランザクションによる読み込み操作
はトランザクション A よりも未来の直列化順序という前提でバッチトランザクションの終わりを待たなくてよくなる．
そのような挙動をさせる場合は新しいバージョンを設けて，そのバージョンに上書きしていく(2VCC)．
Silo が write set に対して max (each (tid + 1)) する理由は read への通知なため（？），そのレコードだけに関してこれを省ける
（場合によっては invisible write にできる）．
新しいバージョンが来てからは，そのバージョンに対して以降通常の Silo cc を適用できる（未来に置く）．
そうなると，batch’s write もそのレコードに対して適用できる（バッチより未来の直列化順序となるトランザクションの blind write によって 
バッチの write が invisible になる）．

- バッチトランザクションは必ずコミットさせるプロパティなので，バッチによる読み込み操作に関する read verify は不要となる．

### プロトコル詳細
ロック A は batch read lock (br lock) , ロック B は batch write lock (bw lock),は silo lock bit によるロックは silo lock と
表記する．
- database record metadata<br>
Silo の並行調停は transaction id word (tidword) によってされる．latest bit / absent bit / lock bit が 1 bit, tid 29 bit, epoch 32 bit
という割り当てが標準的である．
br lock / bw lock は通常の silo lock とプロパティが異なるがこれをアトミックに操作したい．
なぜなら，バッチが br lock を立てるときに concurrent short tx's write に先行された場合, 不正な payload 読み込みになる懸念があり，読み込み
前後で lock bit が立っていないか，tid が変わっていないか等チェックすること（楽観的技術）によってアトミックな読み込みをしなければいけないが，
これは性能を short tx に割り当てる仕様であり，バッチを優先する思想に逆行する．
後続された場合，write がバッチ read を不正にしないために，アボートしなければいけないが，Silo は read verify 終了時点で pre-commit 確定
なため，コミットしたと勘違いして更新した部分をロールバックしなければいけなく，この仕様は後続するトランザクションへ伝搬するため，効果的ではない．
- トランザクション開始時点<br>
バッチトランザクションは同時並行的に一つまでしか走らせないため，これをグローバル引数 boolean batch_executing にて制御する．
トランザクションは enter / leave によってセッション管理されているため，enter の引数でバッチトランザクション用セッションであることを明示し，
batch_executing == true であれば warning を返す．そうでなければセッションを開始する．
- トランザクションオペレーション<br>
  - batch tx phase<br>
  batch は必ずコミットさせるため， read / validation / write phase というようなものは不要とする．local read / write set も後述していく
  プロトコル詳細から不要である．
  - batch's read<br>
  batch tx は必ずコミットするため，read verify 不要である．
  そのため，ローカル read set は作らない．
  br lock をかけて読み込み処理を行う．
  レコードメンバである batch_snap に読み込んだ値を退避させる．
  意図としては， blind write が後続して，その blind write に後続させる形でそのレコードへの CC を許容しているとき，同一 key に対する re-read 
  が（値を退避していない場合，上書きされているため）不可能となるからである．
  代案としてローカル read set を作り，オペレーション実行ごとにそこを検査するというアプローチもあるが，バッチにおいては set size が膨れ上がり，
  検査コストも発散するため現実的ではない．
  batch_snap はコミット時にクリアするとし，読み込み時に batch_snap がクリア状態でなければ有効なバッチ用スナップショット (re-read or read 
  own write) とする．
  従って， batch_snap へのポインタ情報はコミット時にクリアするためにセットとして保持する（が，read/write operation ごとに走査するという
  ようなことはない）．
  batch_snap の br lock bit も立てておくことで，batch_snap が batch read によって生成された印とする．batch_snap は後述するが，batch 
  write によっても生成されうる．
  - short tx's read<br>
  基本的に通常の Silo.
  br lock がかかっていても Silo 的にはロック無しとみなして読み込み処理をする．
  これでうまくいくならば，serialization order はバッチの過去か並置となる．
  bw lock or silo lock がかかっていればそれを解放待ちする．
  bw lock がかかっているときは batch write が latest であるため，バッチが終わるのを待つか， blind write によって上書きされることを
  待たなければいけない．
  - short tx's read verify<br>
  基本的に通常の Silo.
  br lock がかかっていても Silo 的にはロック無しとみなして read verify 処理をする．
  bw lock or silo lock がかかっていれば通常通り検証失敗とみなすし，上書きされていても同様である．
  - batch's write<br>
  通常は bw lock, silo lock を立てて，batch_snap と record body を更新し，tid + 1 する．
  record body 側は silo lock bit を降ろして tidword を更新する．
  bw lock は上がったままなので，これが blind write しか後続できない印となる．
  batch_snap 側も silo lock bit を降ろして bw lock は上がったまま tidword を更新する．
  bw lock が上がったままなので，これは batch_snap が batch_write によって生成された印となる．
  tid + 1 する理由としては short tx's read へ record update を通知する役割と，もし redo recovery と組み合わせるときにそれを可能と
  させるためである．タイムスタンプ更新をしない場合，redo recovery において batch の serialization order がアクセスしたレコードごとに
  another tx と同一となり，順序が不明瞭となる．
  record body 側の bw / silo lock を立てたときに batch_snap がクリア状態ではない（既に batch r or w が走った）とき，4 つのケースがある．
    - batch_snap の br lock bit が立っていて，record body, batch_snap の tid が同一のとき<br>
    batch-read-modify-write であるため，通常通り batch_snap, record body を更新する．
    - batch_snap の br lock bit が立っていて，record body, batch_snap の tid が異なるとき<br>
    blind write により上書きされているため，この write は省略する．
    - batch_snap の bw lock bit が立っていて, record body, batch_snap の tid が同一のとき<br>
    batch-write の後に blind write が来ていない状態であり，通常通り batch_snap, record body を更新する．
    - batch_snap の bw lock bit が立っていて, record body, batch_snap の tid が異なるとき<br>
    batch-write の後に blind write が来ている状態であり，この write は省略する．
  - short tx's write<br>
  基本的に通常の Silo.
  前提として non-deterministic workload を想定しているため， write を打たれた時点でそれが read modify write (RMW), read other write 
  (ROW), blind write (BW) のどれなのかは不明である．
  従って，write ごとに read set を走査して検査しなければいけないかもしれない．
  global boolean batch_executing が false だったら検査しなくてもよいのか？
  false 判定した直後にバッチが走り始めて該当レコードにアクセスするかもしれないし，それを防ぐために batch, worker 間の調停を (worker が多大なるコスト
  を払う形で)するよりも，オンライン処理はショート tx を前提にしているため，線形探索してしまう．
  ただ，ここを高効率化するために特殊なトライロックを実装する．
  このトライロックは bw lock, br lock, silo lock 全てがかかっていないときにのみ成功し，失敗したときは tidword を返し，返された値から
  どのロックが立っているか検査する．
  bw or br lock がかかっていれば blind write だけが許されるので，local read set を検査する．
  RMW であった時点でバッチを待たなければいけないので（ロックを握りしめたまま待つわけにもいかないので）アボートする．
  どちらもかかっていなければ silo lock がかかっている状況ということで，これはショート tx なのでリトライコストも大きくないので No-wait abort 
  アプローチを取る．
  - batch read / write
  read, write ごとに local read / write セットをアペンドしていく．
  それはトランザクション終了時に (blind write に上書きされなかったために) 立てたままである br / bw lock を下げるためであり，
  レコードへのポインタを保持するためである．
  オペレーションごとにセットを走査するということは無いため，アペンドするたびに繰り返される特定の処理コストが上がっていくというようなことは無い．