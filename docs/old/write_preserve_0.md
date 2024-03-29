# Write Preserve 0 Protocol design

## 本ドキュメントの意義

shirakami に施す Write Preserve 0 Protocol 初期案設計に関するドキュメントである。

## 概要

- 用語
- write preserve とは
- wp level 0 基本制約
- 基本制約の詳細
- バッチ処理プロトコル詳細
- オンライン処理プロトコル詳細
- エポックフレームワーク
- 実装最適化
- 考察
- 付録

## 用語

本ドキュメントで用いる用語である。

- 生存区間
  - トランザクション開始から、獲得したロックを全て解放するまでの区間。
- reader
  - 生存区間が重なっているトランザクション間において read / write conflict が発生したとき、transactional read を行っている側のトランザクション。
- writer
  - 生存区間が重なっているトランザクション間において read / write conflict が発生したとき、transactional write を行っている側のトランザクション。
- write preserved
  - セクション "write preserve とは" にて解説する。
- wp
  - write preserve.

## write preserve とは

- バッチトランザクションを貫徹させつつオンライン処理も積極的にコミットさせるためのバッチトランザクションによる write 予報である。
  バッチとオンライン両方を通すためには、悲観的要素が必要である。
  バッチがオンラインに後置となる、2pl like に全てのロックを取りきったところに順序を取る場合、オンライン処理はバッチによる（大規模）ロックの解放を待たなければいけない。
  バッチがオンラインに前置となる場合、オンライン処理はバッチ処理がどこに読み書きするか分からないが、バッチ処理を失敗させないために何もできなくなる。
  そこで、バッチを前置と定め、バッチが write する（可能性のある）領域をあらかじめ宣言 (write preserve) することとする。
  そうすると、オンライン処理は wp 領域を read する、あるいはその領域において特定ページの存在性に依存する操作を行うときのみバッチを考慮すればよいことになる。

## wp level 0 基本制約

本制約は、基本的制約である。

1. バッチの開始順序とトランザクションオーダーは一致する。
1. バッチは、 自身より優先度の高いバッチが宣言する WP について、 read step の対象に取れず、取ろうとしたらアボートする。
1. オンライン処理は wp 領域に対して update, insert などの存在性に依存する操作は許容されず、 upsert のような存在性に依存しない操作のみ許容される。
1. バッチの開始以降に serialization point を持つオンラインがコミットした場合、そのオンラインのトランザクションオーダーはバッチよりも後ろに置く。
1. バッチは epoch base read only snapshot (ROSS) を read する。
1. バッチの read step において、 自身より高優先度の未コミットバッチが宣言する WP と衝突する場合、自身をアボートさせる。
1. 後に始まった(低優先度)バッチによって、先に始まった（高優先度）バッチが棄却されない。
1. オンライン処理は未コミットバッチ write のあるページを read できず、アボートする。
1. wp 有効区間はトランザクション開始時の wp phase で始まり、トランザクションの終了時に終わる。

## 基本制約の詳細

- バッチの開始順序とトランザクションオーダーは一致する。

  - バッチは開始時に共有カウンタから採番し、それを優先度とする。

- バッチは、 自身より優先度の高いバッチが宣言する WP について、 read step の対象に取れず、取ろうとしたらアボートする。

  - 開始順序が優先順序でトランザクションオーダーなので、優先度の高いバッチが前置となる。後置となるバッチは前置となるバッチの write 結果を待たなければいけないが、待つよりかはアボートする。例外として、高優先度バッチがトランザクションを開始して以降、低優先度バッチがトランザクションを開始するより以前にオンライン処理が共通ページに対して書き込みを行っていた場合、低優先度バッチはオンライン処理の結果を読めばよいので、 read step の対象が高優先度バッチとならない。

- オンライン処理は wp 領域に対して update, insert などの存在性に依存する操作は許容されず、 upsert のような存在性に依存しない操作のみ許容される。

  - example 1 : バッチ処理が delete x をしようとしているところ、オンライン処理が update x を実行し、前置としてコミットしてしまったと仮定したとき、バッチ処理かオンライン処理をどちらか棄却しなければいけない。
  - example 2 : バッチ処理が存在しないページ x に対して insert をしようとしているところ、オンライン処理が先に insert x を実行し、前置としてコミットしてしまったと仮定したとき、バッチ処理は insert x が実行できなくなる。
  - 前述した例のような問題において、バッチ処理を優遇するため、オンライン処理は wp 領域に対して存在性に依存する操作を許容されない。

- バッチの開始以降に serialization point を持つオンラインがコミットした場合、そのオンラインのトランザクションオーダーはバッチよりも後ろに置く。

  - オンライン処理がバッチの開始より前にトランザクション開始時点を持つケースも含む。オンライン処理に関しては、silo validation protocol において write lock を取ってしまえば他のオンライン処理に順序が追い抜かされることは無くなる。そのタイミングで最新バッチ id を確認し、それよりも後置となるように検証処理を行う。

- バッチの read step において、 自身より高優先度の未コミットバッチが宣言する WP と衝突する場合、自身をアボートさせる。

  - 衝突する場合というのは、高優先度未コミットバッチの write が read の対象であった時であり、間にコミット済みオンライン処理 write があればそれ（が ROSS になったもの）を適切に読み込む。

- 後に始まった(低優先度)バッチによって、先に始まった（高優先度）バッチが棄却されない。

  - 先に始まったバッチが棄却されえるということは、スタベーションを起こしかねないということである。本設計ではスタベーションを起こさぬよう、先に始まったバッチが高優先度となるように設計している。

- オンライン処理は未コミットバッチ write のあるページを read できず、アボートする。

  - 未コミットバッチの write を read するならば、そのコミットを待たなければいけない。
    しかし、バッチを待つことは長時間になるため、待つよりかはアボートさせる。

- wp 有効区間はトランザクション開始時の wp phase で始まり、トランザクションの終了時に終わる。

  - wp n : wp をより早期に解放するかもしれない。その動機には他の低優先度バッチに早く読ませたい、オンライン処理に存在性に依存する操作を早めに許容させたいということが考えられる。前者は低優先度バッチを遇するケースで、トランザクションオーダーと優先度順が一致しなくなる。後者はコミット済みオンライン処理を棄却できないという前提を敷くとバッチが通らなくなる懸念がある。そのため、今回は wp 早期解放を考慮しない。

## バッチ処理詳細

- wp / read / validation / write phase を持つ。

  - phase 分けをするのは、コストのかかるカスケード（アボート）を避けるためである。
    ステップごとに確定させる、アボートありきのモデルの場合、確定（グローバルに反映）させた write を打ち消す必要がある。
    従って、write が read に与える影響を可能な限り（ロック獲得行為まで）遅延させる。
  - wp phase
    - 排他的に write preservation を行う。
  - read phase
    - read phase はレコードからの読み込み、書き込み準備を行う。
  - validation phase
    - コミット可能か検証を行う。
  - write phase
    - コミット可能な場合、 write 反映処理を行う。

- wp phase

  - logic detail

    - write preservation を行う。
      低優先度バッチの read が高優先度バッチの wp を見逃すことが無いように、wp phase はバッチ間において排他される。
      排他することで、オンライン処理はトランザクション開始時点で開始済みであるバッチの判断がしやすくなる。
      詳細は考察セクション "非同期 write preserve" に記す。

  - protocol detail
    1. バッチ開始用排他ロック (batch_mtx) を獲得する。
    1. バッチ優先度カウンタ (batch_counter) から採番する。
    1. write preservation を行う。
    1. batch_mtx を解放する。

- read / predicate read operation

  - common logic detail
    - epoch base ROSS を読む。
    - 自身より高優先度のバッチによる wp を観測したとき、優先度順がトランザクションオーダー順の制約より、後置となるが、後置（コミット）を待つよりかはアボートする。
    - 後に始まったバッチが先に始まったバッチの wp を見逃すことが無いように、低優先度バッチの read phase は高優先度バッチの wp phase を追い抜かしてはいけない。これは wp phase が排他されていることによって保証される。
    - ROSS がオンラインによってオンデマンド生成されていたら、それを読む。されていなかったらメインバージョンを楽観 read する。楽観 read に失敗したら再度 ROSS チェックから行う。
    - 自身より低優先度のバッチによる wp を観測したとき、その wp にアボートフラグを立て, その低優先度バッチをアボートに促す。
    - ページには読んだ足跡(read by)を残す。これは低優先度 writer がセルフアボートを決定するための情報である。最適化に関しては実装最適化セクションを参照されたい。
    - read by は二種類用意する。一つ目は並行に読まれているかどうか、二つ目はコミット済みの（過去に並行していたかもしれない）ものである。一つ目は writer が高優先度 reader の衝突を検知して待ち合わせするためのものである。二つ目に関しては固定長の入れ物とカーソルを使って、入れ物に順繰りに格納していけばよい。固定長サイズが最大並行バッチ数と定義し、wp phase が排他的であることを利用すると、read by 情報を後から参照したいバッチ writer はそれよりも古い最大並行バッチ数 - 1 だけのバッチからしか読まれえないため、必要な情報が欠損するということは無い。wp phase が排他的であるという特徴から、自身が開始時点に並行していたバッチを特定することが容易であり、不必要に古すぎる情報と検証して偽陽性アボートを頻発させるということもない。
  - predicate read logic detail
    - page set の wp 情報から、自身より高優先度 write と衝突していないかをチェックする。衝突していたら自身をアボートさせる。衝突していなければ predicate 情報を page set に残す。コミットは優先度順に行われるため、後から来る衝突しうる低優先度 writer は必ず validation phase 以降では高優先度 predicate reader によって残された足跡を観測することができ、自身をアボートさせることができる。
  - read protocol detail
    1.  wp をチェック。wp が存在する場合、自身より高優先度なら自身をアボートさせる。自身より低優先度なら相手をアボートさせるフラグを wp に立てる。
    1.  ROSS があればそれを読む。無ければ楽観 read を試みる。楽観 read に失敗したら再度 ROSS チェックから行う。
    1.  読んだ足跡(current read by)を残す。
  - predicate read protocol detail
    1. wp をチェック。wp が存在する場合、自身より高優先度なら自身をアボートさせる。自身より低優先度なら相手をアボートさせるフラグを wp に立てる。
    1. predicate 情報を page set に残す。
    1. ROSS があればそれを読む。無ければ楽観 read を試みる。楽観 read に失敗したら再度 ROSS チェックから行う。
    1. 読んだ足跡(current read by)を残す。

- write operation

  - logic detail
    - read preservation が無いため、いつ自分より高優先度な batch read が来るか分からない。グローバル変数から得られる情報によって、自分より高優先度な並行バッチがすべて終わるのを待ち、 write validation を行う validation phase が必要になる。
      自分より低優先度なバッチの終わりを待つ必要が無い理由は、低優先度によって高優先度がキックされることが無いからである。
    - write operation によっても GC を行う。バッチによるバージョンリストは先頭が新しいものであれば、新しいバッチがリストの古いバージョンにアクセスすることはなくなるため、ロックの所有者は並行動作者を気にかけることなく古いバージョンをメモリ解放できる。オペレーション実行者がロックの所有時間を伸ばしてまでリソース管理に協力するべきか議論があるかもしれないが、バッチ混在システム（ワークロード）ではメモリの枯渇が懸念されるため、積極的にリソース管理を行うという観点で GC に協力する。
    - バッチによるバージョンリストは低優先度（最新）によるものが先頭、高優先度（古いもの）によるものが後方になるようにする。
    - ロックを取った後、（トランザクショナル順序的に）最新バージョンがバッチによるものであれば、それでオンライン処理向けメインバージョンを上書きする選択肢もあるが、それは行わない。なぜならいつ何時バッチが始まるか分からず、高頻度にバッチが来やすいワークロード向けのシステムであるため、オンライン処理は最新バッチとメインバージョンのどちらが読むべき順序かを検討しがちで、メインバージョンを更新しても順序を検討するコストを崩しにくいからである。
    - early validation. ページにはバッチによって読んだ足跡が残されている。自身より高優先度 reader が存在した場合、セルフアボートする。
  - protocol detail
    1.  page に対して gc トライロックをかける。失敗したら gc を見送る。
    1.  gc トライロックに成功していた時、最後にコミットしたバッチ id より古いバージョンリストの要素（これからの並行バッチによってアクセスされえない要素）を GC する。
    1.  gc トライロックに成功していた時、gc ロックを解放する。
    1.  page ロックをかける。
    1.  early validation. ページに対して自身より高優先度 reader が足跡を残していたらセルフアボートを決める。
    1.  バッチ用バージョンリストに自身の更新を追加する。
    1.  page のロックを解放する。

- validation phase

  - logic detail
    - read validation なし。 write validation を行う。
    - いつ高優先度バッチが自身が write した領域に read してくるか分からないため、自分より高優先度のバッチがコミットするのを待つ。
    - 自分より高優先度な (predicate) reader が来たかどうかをチェックする。
    - validation phase を終えたとき、バッチ間の serialization point となる。そこでグローバルバッチカウンタを読み込んで最新の並行バッチのタイムスタンプを把握し、
  - protocol detail
    1. 自身より高優先度（古い）バッチ全てが終了（コミット・アボート）するのを待つ。
    1. 自身より高優先度な (predicate) reader が来たかどうか、 write set をチェックする。

- write phase

  - logic detail

    - 更新処理を行う。
    - 自身より低優先度（後から始まった）バッチが validation で自身を検出できるように、read の足跡を残す。足跡を残す処理と、リーダーが read phase で足跡を読み込む行為は排他せずにアトミックに行えるようにする。

  - protocol detail
    1. ペイロード（value）更新処理を行う。
    1. committed read by の足跡を残す。

- cleanup at commit / abort
  - logic detail
    - グローバルに反映させた一時情報である wp, read by をクリアする。

## オンライン処理詳細

- overall property
  - コミット済み online tx は次のエポック以降始まるバッチより前置となる。
- (predicate) read operation
  - common logic detail
    - 未コミット batch write に後置できず、wp フィロソフィーより前置もできない（アボート）。いかなるフェーズでも wp あるいは未コミット batch write と衝突したらアボートする。
    - occ reader は read phase のレコードアクセスにて wp がかかっていないかつ未コミット batch write が存在しないことを確認し、validation phase でも read verify とともに確認する。
- write operation
  - logic detail
    - ROSS 作成を行う。
    - write は同一エポック batch write に対して後置となる。
    - 任意のトランザクションを閉塞させないアプローチを採用する場合、 tx はどのバッチとどういうタイミングで開始されたかを知りようが無い。
      そのため、存続している並行バッチ全てに対して、ROSS の作成に協力する。
      協力対象となるバッチは serialization point (write lock 後、 read verify 前)にて実行 (read phase) 中の並行バッチである。
      serialization point 以降に始まった並行バッチはオンラインに後置する。
      serialization point 以前に始まっている存続中の前置となる並行バッチ向けに write phase にて ROSS 作成を協力する。
      occ serialization point の性質を保持するために、 write lock を獲得した後に始まったバッチは occ に後置とする。
      後置の仕方としては、バッチ write が occ による write lock を検知したらアンロックされるまで待つ。
    - write preserve, 未コミット batch write されているページに対する書き込みは upsert のみ許容される。
      - example
        - バッチ側が delete して、 オンラインが update したとき、バッチが前置になるため両者を通すことができない。オンライン側を許容しないこととする。
        - バッチ側が insert をしたくて wp をかけていたところ、オンライン処理が insert してしまったため、バッチ側が insert できなくなってしまうというケースが考えられる。オンライン側を許容しないこととする。

## エポックフレームワーク

- batch base ROSS

  - logic detail
    - batch ごとに ROSS を持つ。バッチが通りやすくするということを目標にしているため、 ROSS の作成はバッチではなく主にオンライン処理が協力する。

- clean up manager
  - ROSS
    - バッチ混在ワークロードはメモリ使用量が著しく大きいことが懸念されるため、積極的に gc を行うという観点でオンライン処理もバッチ処理も gc に協力する。

## 実装最適化

- read by
  - wp 0 level においてはテーブル単位で rw conflict を検出する。なので、衝突情報（wp, read by) はともに page set に付与することが効率的である。
  - 管理（探索・GC・登録）コスト削減のため、固定長にする。
    - read by は array<int, 64> とする。要素がバッチ id を示す。
    - 並行バッチ数は最大で read by の最大要素数とする。
    - グローバル変数に read by の最大要素数がビット長の read by pos 変数を設ける。
      これは read by のスロットを予約するためである。
      wp phase は排他されているため、wp phase にいるバッチが read by pos の最も左寄りの 0 ビットを 1 ビットあげ、そのインデックス位置を自身のポジションとする。
      そのインデックス位置と同一の位置の read by を更新していく。
    - クリアできるポジションは自身のポジションである。commit / abort のターミネーション時に read by における自身のスロットをクリアしていき、 read by pos の自身の位置をクリアする。従って、 read by pos のある位置を獲得したトランザクションは、全ての read by においてその位置がクリアされていることが保証される。

## 考察

- conflict level

  - wp level によって、read - write の conflict 程度を変化させることができる。
    例えば、 wp 0 であれば write はテーブルレベル、 read はページレベルでコンフリクト検証が可能である。wp level が上がると、検証はページレベル同士になる。ページレベル同士になることで広がり、活かすことのできたスケジューリングの余地に対して、アルゴリズムの複雑さや計算コストの増大のバランスはいかようになるか？

- epoch base aligned read

  - online
    - オンライン処理がこけ続けたらバッチ処理化するというアイディアがある。
      競合の強い環境下では、バッチ処理が膨大な数になりうる。バッチ処理ごとに ROSS を作成するモデルにおいて、 ROSS の数、メモリ使用量の激しい増大が考えられる。比較して、 epoch base SS (aligned read) ではそのようなことはない。

- garbage collection

  - コミット順序が優先度順（開始順）であれば、最後にコミットしたバッチ以前のバッチ関連リソースを解放可能（一次元 GC が可能）になる。

- 非同期 write preserve
  - write preserve 非同期化に関するデメリットを記す。メリットは同期的ではなくなることとして割愛する。
    オンライン処理は wp の中間状態を観測してはいけない。

## 付録

- 予備アイディア

  - 本セクションでは、現状主要な論理には組み込まれないアイディアを記す。

  - writer の生存区間終了後に reader が読み込みアクセスした場合、優先度を問わず双方を許容する。cf. aligned read。

    - バッチの write を反映させてから、反映を取り消すカスケード処理はコストがかかるので避けたい。
      そのため、バッチのコミットを確定させる前にグローバルな validation phase （動機ポイント）を設ける。
      よって、 reader が生存区間の終了した(並行)バッチの write を読むことは無い。

  - オンライン処理 read をバッチ write に後置可能なプロトコルにおいて。オンライン処理は検証フェーズ以降においてバッチ write preserve しかけている状態を観測してはいけない。
    - 換言すると、オンライン処理は read phase にて write preserve 中の状態を観測しても良い。
      なぜなら read phase でコミットが確定するわけではなく、 validation phase で収拾がつけばよい。
      validation phase でねじれを観測するのは問題である。
      例えば、バッチがページ x, y を書くとして、衝突したら後置しなければいけないオンライン処理が片方の write preserve を見逃したとき、オンライン処理が見逃したページに対して前置となりうる。しかし、観測できたページに対しては後置となりうる。
      それは write preservation が前置（開始時点の順序）を保証する性質からである。
      見逃さないためには、オンライン処理を閉塞させるか、見逃したことを検知する機構が必要である。
      見逃すどころか一切 wp を観測せずにオンラインがコミットまで走り抜けた場合、それは後続バッチに対して前置とする。
      見逃したこと（ねじれ）を検知するためには、 write preserve のフラグが write preserving / write preserved の二種類（仕掛け中・仕掛け済み）あればよい。
      write preserving をオンライン処理が validation phase にて観測したとき、それが write preserved になるのを待機する。
      既に検証済み領域（non-wp) が wp 領域になっているかもしれないので、 wp - verify をやり直す。
      wp verify は二周行い、周回ごとの結果が等価であることを要求する。
      例えば、バッチが x, y に wp をかけるとして、1 : オンラインが x を検証。 2 : バッチが wp を終える。 3 : オンラインが y にて write preserved を観測するかもしれない。この例では１の検証が破綻している。
      しかし、二周検証を行う前提を敷くと、二周目で 1 の破綻を検知できる。
      そして、wp はテーブル単位であること、これはオンライン処理（ショートトランザクション）の検証の話なので、検証する要素数は少なく、二周行うコストは高くないと考える。
