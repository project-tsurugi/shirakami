# benchmarking

## ベンチマーク概要
- bcc_2b: 単一バッチ処理と複数オンライン処理。オンライン処理は確率的にバッチ処理のテーブルを読みに行く。
- bcc_3b: 複数のバッチ処理と複数のオンライン処理。オンライン処理もバッチ処理も確率的にあるバッチ処理のテーブルを読みに行く。
- bcc_3b_st: bcc_3b よりも wp に有利なワークロードで検証を行う。
- bcc_4: バッチ処理のみ。バッチ処理は確率的に他バッチ処理のテーブル領域に書き込みに行く。
- bcc_5: オンライン処理の書き込みがバッチ処理の書き込みと衝突したときにおけるオンライン処理書き込み性能への影響を測定する。
- bcc_6: ガーベッジコレクションが環境に生成されるバージョン数に対して追い付いているかどうかを検証する。
- bcc_7: occ が read by を残すコストを検証する。
- bcc_8: local read set container type を vector / unordered_map で切り替えたときの性能を分析する。
- bcc_9: value 操作は RCU / reader-writer 排他のどちらが良いかを検証する。
- bcc_10: read only mode かそうでないかにおける read only tx の性能を分析する。
- bcc_11: occ のトランザクションサイズが大きいとき、性能がどのように変化するかを分析する。

* Benchmarking (project_root/bench)
  + RocksDB
    - `-DBUILD_ROCKSDB_BENCH=ON`
      * Build project_root/bench/rocksdb_bench.
      * Default: `OFF`
        