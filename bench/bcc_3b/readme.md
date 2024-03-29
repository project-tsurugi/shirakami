# BCC-3b マルチスレッドバッチ (WP-0)

## 目標

* 複数のバッチ処理と複数のオンライン処理を同時に稼働させ、それぞれが十分な性能を発揮する.

## 実施内容

* Write Preservation レベル分け (概要) の `WP-0` を実現したうえで、 オンライン処理とバッチ処理が混在して稼働するようにする。
* バッチ処理はテーブル単位の WP を指定し、オンライン処理や他のバッチは WP 領域を read しようとする場合がある。

## 実施概要

* `BCC-3a` と同様の条件でバッチ処理とオンライン処理を稼働させる。
* オンライン処理は `BCC-2a` と同様の条件で WP 対象領域を read しにいく。 (0%, 10%, 50%)
* バッチ処理は以下の確率で、他バッチの WP 対象領域を read しにいく。
  + 0% (AB0)
  + 10% (AB10)
  + 50% (AB50)
* `BCC-0` と同様のwrite比 (read 50%, 80%, 99%) や、key value サイズを用い、性能を比較する。

## 性能目標

* `AB0` において、バッチは `BCC-3a` と同等の性能で、衝突率を上げるとその分だけ性能劣化する。
* `AB0` において、オンライン処理は `BCC-3a` と同等の性能で、衝突率を上げるとその分だけ性能劣化する。

## 備考
* 本項目までは 2021 年度中に完了させ、2021 年度の残りは性能向上に取り組むか、制約の緩和に取り組むかを検討する。