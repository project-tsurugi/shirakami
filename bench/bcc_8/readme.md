# BCC-8 read only メモリ使用量ベンチマーク

## 目的

local read set container type を vector / unordered_map と切り替えたときの性能を分析する。

## ワークロード（パラメーター）設定
* オンライン処理スレッド数: 112.
* read only. open_scan / read_from_scan にて read を行う。
* 1 table
* 1M recs
* key size 8 bytes, value size 8 bytes. メモリアロケーターコンポーネントの影響を最小化する意図である。

## 外部パラメーター設定

* 外部パラメーターはコマンド引数で gflags によって与えるものとする。
* -d: 実験時間[sec]. データを測定する時間。
* -tx_size: 1 tx 辺りのオペレーション数。
* skew: アクセススキュー。

## 生成するグラフ

* graph.plt によって生成されるグラフ。
* 横軸が tx_size, 縦軸がスループットと最大メモリ使用量, skew 0。 local read set 要素の重複が無いときの性能を比較する。
* 横軸が skew, 縦軸がスループットと最大メモリ使用量。 local read set 要素の重複があればあるほど性能比がどうなるかを分析する。
