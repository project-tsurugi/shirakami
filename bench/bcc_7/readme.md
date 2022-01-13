# BCC-7 read-anti-dependency info cost benchmark

## 目的

occ が read by を残すコストを検証する。

## 内部パラメーター設定

* オンライン処理スレッド数: 112.
* トランザクションサイズ: 10 due to online property
* read only
* テーブル数 1 or 112
* key size 8 bytes, value size 8 bytes. メモリアロケーターコンポーネントの影響を最小化する意図である。
* レコード数 / テーブル数: 1000
## 外部パラメーター設定

* 外部パラメーターはコマンド引数で gflags によって与えるものとする。
* 実験時間（duration[sec]）: データを測定する時間。

## 生成するグラフ

* graph.plt によって生成されるグラフ。
* 横軸がスレッド数、縦軸がスループット。
