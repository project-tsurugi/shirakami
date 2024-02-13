# shirakami における可変なメモリ使用量の推定に関して

## 日付、更新者: 2024/2/8,tanabe

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

- A: 収集していない。
- B: GC の際に、スキャンした時点で正確な数を収集し、出力している。
- C-1: GC の際に各ストレージにおける総エントリ数を正確に収集し、出力している。
- C-2: 各ストレージごとの key サイズを総和して収集している。それをストレージごとの総エントリ数で割ることで、ストレージごとの平均キー長を出力している。
- C-3:
  - GC の際に、バージョンリストを走査していて GC せずにスキップした数をストレージ単位で記録している。その数を当該ストレージにおけるエントリ数で割った値で出力している。すなわち、ストレージごとの平均的なバージョン長である。
  - 先頭バージョンのバリューサイズを収集しており、各ストレージごとにエントリ辺りにおける平均（先頭バージョン）バリューサイズを出力している。
- C-4,5: 収集していない。

### 現時点で未着手な範囲における情報収集とその難易度等について

- A: yakushima のルートノードは n 個の子を持ち、それぞれが m 個の子を持ち、木構造としてそれが階層的に続いている。原始的な観測をするには構造変化（挿入・削除操作）をブロックする必要がある。
- C-2: キー以外のメタデータ。ヘッダーに含まれるバージョンリストへのポインタなど。エントリ数 \* sizeof(Record) などの計算でほぼ正確なメモリ使用量は現状ユーザー側で計算可能。
- C-3: 上記と同様に、平均バージョン長 \* sizeof(Version) などの計算でほぼ正確なメモリ使用量は現状ユーザー側で計算可能。
- C-4: 現状未収集。収集難易度は高くない。GC が追いついているか追いついていないかの参考情報になるかもしれない。
- C-5: 現状未収集。それらの GC は現在 Tx 処理スレッドによるフォアグラウンドGCをしているため、そこで統計情報を記録し、バックグラウンドスレッドがそれを回収しても良いかもしれない。メモリ使用量の推定がどのテーブル・インデックスで多量のメモリを利用しているか分析するためであるならば、その目的に対して本項目の情報は計算コストに見合った利益という観点でほとんど寄与しないと考えられる。

## 出力例

- コマンド実行例: `env GLOG_v=37 SHIRAKAMI_DETAIL_INFO=1 ./test/shirakami-test_tsurugi_issue344_test`
- log level は 37 である。これは shirakami::log_info_gc_stats の値であり、定義の所在は shirakami/include/shirakami/logging.h である。環境変数が SHIRAKAMI_DETAIL_INFO=1 の時に出力される。
- コマンド実行の留意点: 特定のテストフィクスチャにおいて、Teardown 処理前にsleepを入れてこれだけが見れるように出力させた。
- \# storages: 1-B, 総ストレージ数。ストレージごとに json 形式で av_key_size_per_entry, av_len_ver_list_per_entry, av_val_size_per_entry, num_entries, storage_key 情報を出力している。
- av_key_size_per_entry: 1-C-2. ストレージごとの推定平均キー長
- av_len_ver_list_per_entry: 1-C-3. ストレージごとの推定平均バージョン長
- av_val_size_per_entry: 1-C-3. ストレージごとの先頭バージョンバリューサイズの推定平均値
- num_entries: 1-C-1. ストレージごとの総エントリ数
- storage_key: 当該ストレージのキー情報. create_storage でストレージ生成時に与えられていた引数。

```
I0208 18:02:50.661947 3376341 garbage.cpp:380] /:shirakami:detail_info: ===Stats by GC===
I0208 18:02:50.661983 3376341 garbage.cpp:382] /:shirakami:detail_info: # storages: 15
I0208 18:02:50.662119 3376341 garbage.cpp:397] /:shirakami:detail_info: {"av_key_size_per_entry":1,"av_len_ver_list_per_entry":1,"av_val_size_per_entry":9,"num_entries":2,"storage_key":"__system_sequence"}
I0208 18:02:50.662413 3376341 garbage.cpp:397] /:shirakami:detail_info: {"av_key_size_per_entry":0,"av_len_ver_list_per_entry":0,"av_val_size_per_entry":0,"num_entries":0,"storage_key":"CHAR_TAB"}
I0208 18:02:50.662683 3376341 garbage.cpp:397] /:shirakami:detail_info: {"av_key_size_per_entry":0,"av_len_ver_list_per_entry":0,"av_val_size_per_entry":0,"num_entries":0,"storage_key":"INT4_TAB"}
I0208 18:02:50.662951 3376341 garbage.cpp:397] /:shirakami:detail_info: {"av_key_size_per_entry":1,"av_len_ver_list_per_entry":1,"av_val_size_per_entry":9,"num_entries":2,"storage_key":"NON_NULLABLES"}
I0208 18:02:50.663267 3376341 garbage.cpp:397] /:shirakami:detail_info: {"av_key_size_per_entry":0,"av_len_ver_list_per_entry":0,"av_val_size_per_entry":0,"num_entries":0,"storage_key":"T0"}
I0208 18:02:50.663534 3376341 garbage.cpp:397] /:shirakami:detail_info: {"av_key_size_per_entry":0,"av_len_ver_list_per_entry":0,"av_val_size_per_entry":0,"num_entries":0,"storage_key":"T1"}
I0208 18:02:50.663810 3376341 garbage.cpp:397] /:shirakami:detail_info: {"av_key_size_per_entry":1,"av_len_ver_list_per_entry":1,"av_val_size_per_entry":9,"num_entries":3,"storage_key":"T10"}
I0208 18:02:50.664088 3376341 garbage.cpp:397] /:shirakami:detail_info: {"av_key_size_per_entry":0,"av_len_ver_list_per_entry":0,"av_val_size_per_entry":0,"num_entries":0,"storage_key":"T2"}
I0208 18:02:50.664373 3376341 garbage.cpp:397] /:shirakami:detail_info: {"av_key_size_per_entry":0,"av_len_ver_list_per_entry":0,"av_val_size_per_entry":0,"num_entries":0,"storage_key":"T20"}
I0208 18:02:50.664644 3376341 garbage.cpp:397] /:shirakami:detail_info: {"av_key_size_per_entry":0,"av_len_ver_list_per_entry":0,"av_val_size_per_entry":0,"num_entries":0,"storage_key":"TDECIMALS"}
I0208 18:02:50.664963 3376341 garbage.cpp:397] /:shirakami:detail_info: {"av_key_size_per_entry":0,"av_len_ver_list_per_entry":0,"av_val_size_per_entry":0,"num_entries":0,"storage_key":"TSECONDARY"}
I0208 18:02:50.665236 3376341 garbage.cpp:397] /:shirakami:detail_info: {"av_key_size_per_entry":0,"av_len_ver_list_per_entry":0,"av_val_size_per_entry":0,"num_entries":0,"storage_key":"TSECONDARY_I1"}
I0208 18:02:50.665508 3376341 garbage.cpp:397] /:shirakami:detail_info: {"av_key_size_per_entry":0,"av_len_ver_list_per_entry":0,"av_val_size_per_entry":0,"num_entries":0,"storage_key":"TSEQ0"}
I0208 18:02:50.665772 3376341 garbage.cpp:397] /:shirakami:detail_info: {"av_key_size_per_entry":0,"av_len_ver_list_per_entry":0,"av_val_size_per_entry":0,"num_entries":0,"storage_key":"TSEQ1"}
I0208 18:02:50.666047 3376341 garbage.cpp:397] /:shirakami:detail_info: {"av_key_size_per_entry":0,"av_len_ver_list_per_entry":0,"av_val_size_per_entry":0,"num_entries":0,"storage_key":"TTEMPORALS"}
```

### 出力例におけるメモリ使用量の推定方法に関して
- 本セクションで少なくとも使用されているメモリ使用量の計算に関して詳述する。
- 計算
  - ストレージごとに計算し、全ストレージの総和が少なくとも使用されているメモリ使用量になる。
  - 各ストレージの計算: num_entries \* (sizeof(shirakami::Record) + av_key_size_per_entry + av_val_size_per_entry) + av_len_ver_list_per_entry \* sizeof(shirakami::version) [byte]
    - num_entries \* (sizeof(shirakami::Record) + av_key_size_per_entry + av_val_size_per_entry): ヘッダーデータに要するメモリ量
    - av_len_ver_list_per_entry \* sizeof(shirakami::version): バージョンリストに要するメモリ量
  - sizeof(shirakami::Record), sizeof(shirakami::version) の具体的な値は shirakami 起動時に一度だけ LOG(INFO) で出力されている。
  ```
  I0208 18:06:04.290099 3376879 garbage.cpp:31] /:shirakami sizeof(Record): 320, sizeof(version): 128
  ```