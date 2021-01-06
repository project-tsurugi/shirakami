# Read & report for shirakami

執筆開始 2020/12/15 tanabe

## 本ドキュメントの意義

shirakami に対して read & report 形式でバッチ対策を行うことに関するドキュメントである。

## 概要

-   read & report とは
-   table set A/B の分割方式と benchmark
-   protocol detail
-   epoch framework

## read & report とは

バッチ処理であるロングトランザクションをアボートさせずに処理しつつ、並行してオンライン処理であるショートトランザクションを高性能に処理するための機構である。
ロングトランザクションは serialization order においてトランザクション開始時点を serialization point とする。オンライン処理とバッチ処理で書き込み先を変えることによってオンライン処理は迅速に処理されることが実現できる。書き込み先の分け方を下記テーブルに記す。

|             |             short tx            |        long tx        |
| :---------: | :-----------------------------: | :-------------------: |
| table set A | tx read, tx write, read only tx | tx read, read only tx |
| table set B |           read only tx          |        tx write       |

short tx は書き込み先が long tx の書き込み対象とならないことが分かっているため、long tx の書き込みを潰す心配が無い。long tx は読み込みうるため、long tx が始まってからレコードに対する最初の書き込みは古い値をスナップショットとして退避させ、long tx が読み込めるようにしておく。short tx が table set B を読むことを許容するのは read only tx だけであり、table set B の古いスナップショットを読み込むことで long tx との conflict relation を持たないようにする。

-   制約
    -   read only tx は古いスナップショットを見ることで、read verify も無く高速に処理したいため、トランザクション開始時にヒントを与えられたい。与えられなかった場合、通常の read protocol に従って最新バージョンを読んで、後に適切かどうか検証する必要がある。

## table set A/B の分割方式と benchmark

テーブル集合の分割はキー空間の分割によって模擬する。ワークロードをどのように決定するかに関して、成果の有効性を最大限に確認できるような設定方式を記す。

-   ベースライン  
    経験的に CC のみ、ショートトランザクションでアボート率 50 % のワークロードを探り、ベースラインとする。これは後述するグラフの性能変遷を観察しやすくするためである。
-   ターゲットとするグラフ  
    横軸にロングトランザクションのオペレーション数、縦軸に性能指標を取り、通常の Silo ではロングトランザクションが全く通らなくなるが、new protocol では定常的に処理できるグラフをターゲットとする。

## protocol details

### record design

main record と、バッチから読み込まれても良いように退避するためのスナップショットレコード(batch_snap)を合わせて 2 record と設計する。

### epoch framework

-   philosophy  
    epoch は基本的に通常の Silo と同じフレームワークとする。epoch の進行は long tx を無視して行う。そうすることで、可能な限り GC が詰まらないようにする。epoch の進行とバッチ進行を切り離したので、バッチは開始時点と終了時点の epoch 情報があるべきであり、それらが同一であった時のバッチ間を区別するためにバッチが何回目なのかのカウンタが必要である(i.e. バッチ用カウンタの最大値数だけ同一 epoch でバッチが走れる)。

| bits layout |      25 bits      |     25 bits     |    13 bits    |      1 bits     |
| :---------: | :---------------: | :-------------: | :-----------: | :-------------: |
| batch_epoch | batch_start_epoch | batch_end_epoch | batch_counter | batch_executing |

-   詳細  
    グローバル変数に 64 bits 変数（batch_epoch）を設け、第一上位 25 bits を最後に long tx が始まった epoch (batch_start_epoch), 第二上位 25 bits を最後に long tx が終わった epoch とする(batch_end_epoch)。第三上位 13 bits をロングトランザクション実行回数カウンタとする(batch_counter、初期値 1)。最下位 1 bit をロングトランザクションが実行中かどうかとする(batch_executing)。

    -   batch_start_epoch  
        GC と long tx による table set A に対する read only tx のためである。これは単調増加とし、long tx が終了するときにクリアしたりはしない。仮にクリアする扱いをすると、 GC できるエポックを決定するときに、クリア状態を観測した直後に同じエポックで long tx が走り始めたりしたら seg v が起きかねない。同様に、実行しているかどうか (batch_executing) 情報とセットでアトミックに扱わなければ同様のトラブルが発生しかねない。また、 long tx が table set A に対して read only tx をするときに、safely snapshot の view を壊されないようにするためである。
    -   batch_end_epoch  
        short tx が table set B に対して read only tx を行う際の safely snapshot のためである。
    -   batch_counter  
        定義域は本質的に 1 epoch の中で何回までロングトランザクションを走らせられるかを意味する。これはバッチ用スナップショットを退避させるときに付帯させた batch_epoch があるとして、batch_start epoch, batch_end_epoch が同一だったときに、同一エポック内にて次のロングトランザクションを処理する際に遭遇した batch_snap がいつ何のために退避されたのかが不明瞭になる。それを回避するため、batch_counter を設ける。
    -   batch_executing  
        バッチトランザクションが実行中かどうかを示す。

-   エンジン外部に対する制約    
    -   オンライン処理が table set B に対する read only tx はオンライン処理を想定しているため、バッチ処理を超える処理時間がかかるような処理を許容しない。そのような場合、record body, snapshot の 2 version だけでは一貫した状態を掴みにくい状況に陥りかねない。
    -   同一 epoch で走ってよい総バッチ数は batch_counter の最大値まで。例えば、batch_counter 一週目の 0 と二週目の 0 は ABA 問題となるが、区別できる機構が無い。本ドキュメントでは epoch は Silo オリジナル設定の 40ms としたときに、40 ms 以内に batch が 2^13 回完遂することは現実的に不可能であろうという前提を置いて設定した。
-   エンジン内部に対する制約

    -   batch_counter の初期値は 1 とする。初期値が 0 の場合、 epoch 0 の時点において、batch_snap.batch_epoch の初期状態と一回目の更新された状態の区別がつかなくなってしまう。

-   Future work
    -   partitioned batch_epoch  
        batch_epoch をパーティション分割して、レコードに持たせるバッチスナップ数もパーティション数だけ設けて、それぞれのスロットを対応させる。バッチトランザクションを並行して走らせられるようになる。
        -   制約  
            **[old] バッチトランザクション同士の書き込み先が互いに素である。** [new] バッチトランザクション同士の書き込み先は素でなくても良い？ A を読んで B を書くトランザクションが二つ並列に動作していたとして、任意のオーダーで serializable. 

|      partition bits     |      32 bits      |      32 bits      |
| :---------------------: | :---------------: | :---------------: |
| partitioned batch_epoch | batch_epoch_slot1 | batch_epoch_slot2 |

### transaction operation

-   short tx read  
    Silo protocol に準ずる。
-   short tx write  
    Silo protocol における serialization point (epoch 取得) のタイミングで batch_epoch を取得する。batch_epoch から batch_start_epoch, batch_end_epoch, batch_executing を抽出し、batch_executing && (batch_epoch != batch_snap.batch_epoch) であれば batch_snap を更新する。

-   long tx read  
    batch_snap.batch_epoch == global batch_epoch であれば、batch_snap から読み込む。そうでないなら main record から Silo 方式で読み込む。 write に割り込まれて読み込みに失敗したとしたら write によって batch_snap が更新されているはずなので、lock の解放待ちをした後にそちらから読み込む。

-   long tx write  
    batch はオンライン処理に比べて長い時間走る前提を置いているため、batch_snap は write ごとに更新する。換言すれば、2 version で運用するが、 2 version 目はオンライン処理による read only tx のためである。
