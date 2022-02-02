# BCC-10 (read only / not read only ) mode における read only benchmark

## 目的

read only mode かそうでないかにおける read only tx の性能を分析する。

## ワークロード（パラメーター）設定
* スレッド数 (28 56 84 112). x 軸としてスイープさせる。なぜならメタデータ更新に伴う性能劣化のケースが存在するからである。
(ex. skew 0.99, 10 recs, many worker threads).
* read 100%
* 1 table
* 1M recs
* 1 operation / tx
* skew 0
* key size 8 bytes, value size 8 bytes. メモリアロケーターコンポーネントの影響を最小化する意図である。

## 外部パラメーター設定

* 外部パラメーターはコマンド引数で gflags によって与えるものとする。
* -d: uint64_t: 実験時間[sec]. データを測定する時間。
* -read_only: bool: read only かどうか。
* -th: uint64_t: ワーカースレッド数.

## 生成するグラフ

* graph.plt によって生成されるグラフ。
* 横軸がスレッド数, 縦軸がスループット. それぞれの mode で比較する。
