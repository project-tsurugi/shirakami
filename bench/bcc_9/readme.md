# (old benchmark) BCC-9 value 操作戦略に関するベンチマーク

## 目的

value 操作は RCU / reader-writer 排他のどちらが良いかを検証する。

## ワークロード（パラメーター）設定
* スレッド数 (28 56 84 112)
* read 50%, write 50%
* 1 table
* 1M recs
* 10 operation / tx
* skew 0
* key size 8 bytes, value size 8 bytes. メモリアロケーターコンポーネントの影響を最小化する意図である。

## 外部パラメーター設定

* 外部パラメーターはコマンド引数で gflags によって与えるものとする。
* -d: 実験時間[sec]. データを測定する時間。
* -th: ワーカースレッド数.

## 生成するグラフ

* graph.plt によって生成されるグラフ。
* 横軸がスレッド数, 縦軸がスループットと最大メモリ使用量. それぞれの戦略で比較する。
