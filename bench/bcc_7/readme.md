# BCC-7 read-anti-dependency tracking cost (by occ) benchmark

## 目的

occ が read by を残すコストを検証する。

## 内部パラメーター設定

* オンライン処理スレッド数: 112.
* トランザクションサイズ: 1 
* read only
* テーブル数 1
* key size 8 bytes, value size 8 bytes. メモリアロケーターコンポーネントの影響を最小化する意図である。

## 外部パラメーター設定

* 外部パラメーターはコマンド引数で gflags によって与えるものとする。
* -d: 実験時間[sec]. データを測定する時間。
* -rec: レコード数.

## 生成するグラフ

* graph.plt によって生成されるグラフ。
* 横軸がレコードサイズ、縦軸がスループット。
