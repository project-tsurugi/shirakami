# Tests about concurrency control

## Directory structure
- hybrid
  - Tests about a workload: short tx + long tx.
- long_tx
  - Tests about a workload: long tx only.
- short_tx
  - Tests about a workload: short tx only.

# テストファイル名の命名規則

- ディレクトリ名がオペレーション名(ex. delete)と同等である場合、そのディレクトリに
含まれるテストファイルはそのオペレーションの挙動をテストすることが目的である。
- ディレクトリ名にオペレーション名が複数含まれる場合（ex. delete_insert), その
ディレクトリに含まれるテストファイルはそれらのオペレーションによる相互作用をテスト
することが目的である。
現状は上述した通りの分類なため、ディレクトリ以下にはそれらのオペレーションが同一TXに
含まれるケース、異なるTXによるものであるケース、オペレーション順序を考慮したケース
などがある。
オペレーション名の配置はアルファベット順にアンダースコアで連結したものとする。例えば、
scan と upsert との相互作用を検証したい場合、命名はアルファベット順に並べたものを
アンダースコアで連結した scan_upsert とする。
- 各テストケースの先頭には short_ / long_ / hybrid_ といった文字列をつける。目的は short_insert /
long_insert / hybrid_insert などのようにファイル名の衝突を避ける意図がある。

## ファイル名に用いられる用語解説

- diff_epoch
  - 異なる epoch で始まったトランザクション同士の相互作用をテストしている。
- same_epoch
  - 同一 epoch で始まったトランザクション同士の相互作用をテストしている。
- diff_key
  - 異なる key を操作対象としたトランザクション同士の相互作用をテストしている。
- same_key
  - 同一 key を操作対象としたトランザクション同士の相互作用をテストしている。