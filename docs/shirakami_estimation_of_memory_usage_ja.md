# shirakami における可変なメモリ使用量の推定に関して

## 日付、更新者: 2026-09-28 ban

## 本ドキュメントの目的
  - shirakami における可変なメモリ使用量の推定において、収集しえる情報に関してまとめ、メモリ使用量推定に役立てる。可変というフレーズの意図は、ワークロード状況によって変化することを指しており、任意のワークロードで必ず必要で不変なメモリ使用量(ex. 静的領域)に関しては関知しない。
  - また、現在は主にインデックスに格納されたデータのメモリ使用量について計測を行っており、その詳細と確認方法について示す。

## shirakami における可変なメモリ使用量の種別

- A. インデックスとして用いる yakushima の木構造に用いられているノードの数とそれに含まれた情報。OCCによる操作とLTXによる操作で違いはない。
- B. ストレージ数。yakushima は masstree の中に masstree を格納している。上層がテーブル集合のように機能し、下層がテーブル単体のように機能している。ストレージ数とはこのテーブル集合に含まれたテーブルの数である。OCCによる操作とLTXによる操作でメモリ使用量に違いはない。
  - tsurugidb ではプライマリインデックスの数とセカンダリインデックスの数の和がこのストレージ数になる。tsurugidb ユーザーが情報の metrics を表示する際には jogasaki を間に挟むことで、これらと index / table の対応付けを行う予定である。
- C. ストレージごとの情報
  - 1.  (key-value)エントリ数。エントリとはトランザクション理論でいうページに相当する。バージョンリストのヘッダーデータの役割をしている。OCCによる操作とLTXによる操作でメモリ使用量に違いはない。
  - 2. エントリに含まれる情報.OCCによる操作とLTXによる操作でメモリ使用量に違いはない。
  - 3. エントリに連なるバージョンリストに含まれる情報. OCCによる操作とLTXによる操作でメモリ使用量に違いはない。
  - 4. 任意の Tx からはアクセス不能だが、依然として解放されていない GC コンテナに格納された情報. OCCによる操作とLTXによる操作でメモリ使用量に違いはない。
  - 5. wp, wp の結果, read information. OCCによる操作とLTXによる操作でメモリ使用量に違いがある。これらはLTXによって生成・操作・削除(forground gc)されるメモリ領域である。

### メモリ使用量の影響レベル等について
- 本節では前述したメモリ使用量の種別項目において、影響度の大きさについて述べる。
  - A: 内部ノードとリーフノードのクラスサイズと、それぞれの個数がメモリ使用量になる。shirakami におけるメモリ使用量の多くを占める。
  - B: アクセスパターンが均一、アクセススキューが０に近い場合に、A のメモリ使用量についてストレージレベルで概算する目安になる。
  - C-1: ヘッダークラスサイズと、エントリ数を乗じた値がメモリ使用量になる。ヘッダーの中における可変なメモリ量はキーサイズであるため、長大なキーサイズで多量のエントリを挿入しているワークロードでは大きな影響度合いになる。
  - C-2: C-1 と同様。
  - C-3: 各バージョンにはそのバージョンに合致したバリュー情報が含まれる。そのため、長大なバリューサイズを扱ったワークロードでは大きな影響度合いになる。
  - C-4: GCコンテナにはエントリ、バージョンが含まれる。そのため、それらの影響度合いが大きいワークロードで本項目の影響度も大きくなりうる。また、GC 処理が追いついていないワークロードでも影響度が大きくなる。
  - C-5: shirakami では同時並行できるTx数に限りがある。そのため、wp, wpの結果について、ある時点における生存すべき項目数はその数から大きく乖離しないため、影響度は小さい。read information は影響度が大きくなりうる。例えば、ある時点における生存すべき項目数により多くの read information があればあるほど、生存すべきメモリ使用量が大きくなる。具体的なワークロード例で言えば、大きなテーブルサイズに対してフルスキャンするような read write ltx が多数存在するワークロードなどである。

### 現状収集し、出力している情報とその粒度

- A: diagnostic 表示の最後に yakushima の全 Storage についてメモリ使用量の積み上げ集計を glog に出力している。
- B: GC の際に、スキャンした時点で正確な数を収集し、出力している。
- C-1: GC の際に各ストレージにおける総エントリ数を正確に収集し、出力している。
- C-2: 各ストレージごとの key サイズを総和して収集している。(それをストレージごとの総エントリ数で割ることで、ストレージごとの平均キー長も出力している。)
  - key バイト列のメモリ使用量は std::string SSO 境界を超えるかどうかが境界となるため、その境界を越えたものを別途カウントしている。
- C-3:
  - GC の際に、バージョンリストを走査していて GC せずにスキップした数をストレージ単位で記録している。その数を当該ストレージにおけるエントリ数で割った値で出力している。すなわち、ストレージごとの平均的なバージョン長である。
  - 全バージョンのバリューサイズを収集しており、各ストレージごとにエントリ辺りにおける平均バリューサイズを出力している。
  - バリューバイト列のメモリ使用量は std::string SSO 境界を超えるかどうかが境界となるため、その境界を越えたものを別途カウントしている。
- C-4,5: 収集していない。

### 現時点で未着手な範囲における情報収集とその難易度等について

- C-2: キー以外のメタデータ。ヘッダーに含まれるバージョンリストへのポインタなど。総和を計算し表示している。
- C-3: 上記と同様に、総和を計算し表示している。
- C-4: 現状未収集。収集難易度は高くない。GC が追いついているか追いついていないかの参考情報になるかもしれない。
- C-5: 現状未収集。それらの GC は現在 Tx 処理スレッドによるフォアグラウンドGCをしているため、そこで統計情報を記録し、バックグラウンドスレッドがそれを回収しても良いかもしれない。メモリ使用量の推定がどのテーブル・インデックスで多量のメモリを利用しているか分析するためであるならば、その目的に対して本項目の情報は計算コストに見合った利益という観点でほとんど寄与しないと考えられる。

## 出力例

- コマンド実行例: `env GLOG_v=37 SHIRAKAMI_DETAIL_INFO=1 SHIRAKAMI_REDUCE_GC=0 test/shirakami-test_mem_stat_test --gtest_filter=mem_stat_test.demo`
- log level は 37 である。これは shirakami::log_info_gc_stats の値であり、定義の所在は shirakami/include/shirakami/logging.h である。環境変数が SHIRAKAMI_DETAIL_INFO=1 の時に出力される。
- 書き込みがなかった Storage への Record GC スキップが有効になっていると GC 時の表示が少なくなってしまうので、(必要に応じて) SHIRAKAMI_REDUCE_GC=0 を設定して常に Record GC を実行するようにする。
- コマンド実行の留意点: 特定のテストフィクスチャにおいて、Teardown 処理前にsleepを入れてこれだけが見れるように出力させた。
- \# storages: 1-B, 総ストレージ数
- ストレージごとに json 形式で keys, record_allocated, storage_key, values, version_allocated, yakushima_storage_key 情報を出力している。
    - 従来表示との互換性のため av_key_size_per_entry, av_len_ver_list_per_entry, av_val_size_per_entry, num_entries についても情報を出力している。
- keys
    - num: C-1. ストレージごとの総エントリ数
    - sum_size: C-2. ストレージごとの推定平均キー長。メモリ使用量の算出には直接使用しない
    - ext_num: 上記 num のうち std::string SSO を超えるサイズのものの数
    - sum_ext_size: 上記 sum_size のうち std::string SSO を超えるサイズのものの長さの総計 (ヒューリスティックで切り上げしている)
- values
    - num: C-1. ストレージごとの各エントリの総バージョン数
    - sum_size: C-2. ストレージごとの各エントリの推定平均バリュー長。メモリ使用量の算出には直接使用しない
    - ext_num: 上記 num のうち std::string SSO を超えるサイズのものの数
    - sum_ext_size: 上記 sum_size のうち std::string SSO を超えるサイズのものの長さの総計 (ヒューリスティックで切り上げしている)
- record_allocated: Record 構造体の総使用量 (= sizeof(Record) * keys.num)
- version_allocated: version 構造体の総使用量 (= sizeof(version) * values.num)
- storage_key: 当該ストレージのキー情報. create_storage でストレージ生成時に与えられていた引数。jogasaki のテーブル定義とぶつけることができる。
- yakushima_storage_key: yakushima のストレージキー情報. yakushima の mem_usage_display_all() の出力とぶつけるために表示している
    - yakushima 側は ヘキサダンプで表示しているため、変換のうえぶつける必要がある
- 従来との互換性のために残してある項目
    - av_key_size_per_entry: 1-C-2. ストレージごとの推定平均キー長 (= keys.sum_size / keys.num)
    - av_len_ver_list_per_entry: 1-C-3. ストレージごとの推定平均バージョン長 (= values.num / keys.num)
    - av_val_size_per_entry: 1-C-3. ストレージごとのバージョンバリューサイズの推定平均値 (= values.sum_size / values.num)
        - (以前のバージョンでは先頭バージョンについてのみだったが全バージョンについてに変更している)
    - num_entries: 1-C-1. ストレージごとの総エントリ数 (= keys.num)

```
I0928 18:33:32.670809 158640 garbage.cpp:456] /:shirakami:detail_info: ===Stats by GC===
I0928 18:33:32.670912 158640 garbage.cpp:458] /:shirakami:detail_info: # storages: 5
I0928 18:33:32.671159 158640 garbage.cpp:492] /:shirakami:detail_info: {"av_key_size_per_entry":2.9,"av_len_ver_list_per_entry":1.0,"av_val_size_per_entry":2.9,"keys":{"ext_num":0,"num":100,"sum_ext_size":0,"sum_size":290},"num_entries":100,"record_allocated":25600,"storage_key":"SIMPLE","values":{"ext_num":0,"num":100,"sum_ext_size":0,"sum_size":290},"version_allocated":12800,"yakushima_storage_key":"\u0000\u0000\u0000\u0000\u0001\u0000\u0000\u0000"}
I0928 18:33:32.671391 158640 garbage.cpp:492] /:shirakami:detail_info: {"num_entries":0,"storage_key":"EMPTY","yakushima_storage_key":"\u0000\u0000\u0000\u0000\u0002\u0000\u0000\u0000"}
I0928 18:33:32.671638 158640 garbage.cpp:492] /:shirakami:detail_info: {"av_key_size_per_entry":2.9,"av_len_ver_list_per_entry":2.7,"av_val_size_per_entry":2.4444444444444446,"keys":{"ext_num":0,"num":100,"sum_ext_size":0,"sum_size":290},"num_entries":100,"record_allocated":25600,"storage_key":"MULTI_VER","values":{"ext_num":0,"num":270,"sum_ext_size":0,"sum_size":660},"version_allocated":34560,"yakushima_storage_key":"\u0000\u0000\u0000\u0000\u0003\u0000\u0000\u0000"}
I0928 18:33:32.672012 158640 garbage.cpp:492] /:shirakami:detail_info: {"av_key_size_per_entry":82.9,"av_len_ver_list_per_entry":1.0,"av_val_size_per_entry":82.9,"keys":{"ext_num":100,"num":100,"sum_ext_size":8800,"sum_size":8290},"num_entries":100,"record_allocated":25600,"storage_key":"LONG_PREFIX_KV","values":{"ext_num":100,"num":100,"sum_ext_size":8800,"sum_size":8290},"version_allocated":12800,"yakushima_storage_key":"\u0000\u0000\u0000\u0000\u0004\u0000\u0000\u0000"}
I0928 18:33:32.672395 158640 garbage.cpp:492] /:shirakami:detail_info: {"av_key_size_per_entry":82.9,"av_len_ver_list_per_entry":1.0,"av_val_size_per_entry":82.9,"keys":{"ext_num":100,"num":100,"sum_ext_size":8800,"sum_size":8290},"num_entries":100,"record_allocated":25600,"storage_key":"LONG_SUFFIX_KV","values":{"ext_num":100,"num":100,"sum_ext_size":8800,"sum_size":8290},"version_allocated":12800,"yakushima_storage_key":"\u0000\u0000\u0000\u0000\u0005\u0000\u0000\u0000"}
```

### 出力例におけるメモリ使用量の推定方法に関して
- 本セクションで少なくとも使用されているメモリ使用量の計算に関して詳述する。
- 計算
  - ストレージごとに計算し、全ストレージの総和が少なくとも使用されているメモリ使用量になる。
  - 各ストレージの計算: record_allocated + version_allocated + keys.sum_ext_size + values.sum_ext_size [byte]
    - record_allocated: ヘッダーデータに要するメモリ量
    - version_allocated: バージョンリストに要するメモリ量
    - keys.sum_ext_size: キー文字列が std::string SSO に収まらず動的に割り当てているメモリ量の推定値
    - values.sum_ext_size: バージョンごとのバリュー文字列が std::string SSO に収まらず動的に割り当てているメモリ量の推定値
