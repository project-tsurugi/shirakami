# Engineering optimizations

## この文書について

* Tx エンジン Shirakami において行っているエンジニアリング的な最適化をまとめる。
* 現状の ASIS を示したものであり、適宜更新する。

## atomically load write preserve
write preserve が可変長データ構造だと、データアクセス競合で問題を起こさないために排他が必要になり、甚大な性能劣化を発生させる。従って、固定長データ構造を用いることでメモリ読み込み違反などの実行時エラーを避けることを実現した。
固定長データ構造の長さは最大並行セッション数 (KVS_MAX_PARALLEL_THREADS)となる。

## optimistic write preserve
write preserve は素直に実装すると、共有データに対して競合する読み書きのため排他機構が必要になる。
素直に reader - writer lock 等を用いると、 LTX がないワークロードでも読み込みロックの参照カウントが変化することで、 OCC トランザクションの処理性能が劣化することが自明である。
そもそも shirakami のフィロソフィーは state-of-the-art なショートトランザクション処理性能をショートトランザクションワークロードで発揮しつつ、ロングトランザクション混在ワークロードでロングトランザクションがアボートし続けることなくさばくことであり、ショートトランザクション性能を劣化させることはフィロソフィーに反する。
そこで、 shirakami では optimistic write preserve を提案し、設計・実装している。
write preserve と楽観ロックを組み合わせた機構である。
読み込み側は楽観ロックのタイムスタンプを読み、write presreve を読み、再度楽観ロックのタイムスタンプを読み込むことでアトミックな write preserve の読み込みを保証する。
書き込み側は楽観ロックの書き込みロックを取得し、write preserve を更新し、書き込みロックを解放することでアトミックに書き込む。

## scan 高速化
* open_scan
  + open_scan を実施したとき、先頭の読みえないものを事前にスキップする機構がある。しかし、スキップした箇所においてもマスツリー的な構造変化の検知が必要になる。この時、スキップした要素数のマスツリーノード情報を記録することがナイーブだが、shirakami ではマスツリーノード情報を軽量に分析して、重複を排除して記録してる。
  
## write preserve preserve
write preserve は素直に実装すると、write preserve 宣言の前後OCCトランザクション群を明確に区別するためにOCCトランザクションと write preserve で排他が必要になる。
OCCトランザクションのそれぞれが必ず排他を利用することは自明に深刻な性能劣化となる。
そこで、 shirakami では write preserve presreve を提案し、設計・実装している。
LTX はトランザクション開始時の処理を終えるまで global epoch のロックを取り、 global epoch を動かさないようにする。
そして、 write presreve は必ず将来のエポックのもので宣言し、それ以降にトランザクション処理可能（スタートエポック）とする。
そうすることで、有効な write presreve を OCC は排他を使わずに必ず観測可能になる。
