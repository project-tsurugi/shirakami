# アボート理由に関して

## tanabe, 2022/12/6, 2023/10/17

- 本資料は reason_code (アボート理由)の階層構造と、特定のアボート理由に対して新規情報取得のための調査を追加で行わずに取れる情報をまとめる。各箇条書きにおいて、レベルが深くなるごとにエラー種別のカテゴリが分かれ、最下レベルにおいて返せる情報やそれに関する備考をまとめる。

- 現状、下記返せる情報を返しているとは限らない。議論に応じて加筆・洗練化させていく。

- UNKNOWN
  - 状況: アボート理由が取得できなかった。理由を取得し損ねているバグがある。
- kvs error
  - KVS_DELETE
    - 状況: delete が key なしで実行できなかった
    - 返せる情報：Storage id. delete 操作を試みた key string
  - KVS_UPDATE
    - 状況: update が key なしで実行できなかった
    - 返せる情報：Storage id. delete / update 操作を試みた key string
  - KVS_INSERT
    - 状況： insert key が存在するがために実行できなかった
    - 返せる情報：Storage id. insert 操作を試みた key string
- cc error
  - OCC (all cc:occ error is read error)
    - fail read verify
      - CC_OCC_READ_VERIFY
        - 状況： occ の read phase で読み込んだ値が、 commit phase においては committed write によって上書きされていた。
      - 返せる情報： read 操作を試みた key string
    - write preserve verify
      - CC_OCC_WP_VERIFY
        - 状況：実行中の LTX による write preserve を観測した。
        - 返せる情報:読み込み先で、wp が存在していた Storage 情報。
        - 備考：どの read 操作に関してかは分からない。 wp verify は std::unique や std::sort を用いてテーブルレベルに圧縮しているため。
    - phantom avoidance
      - CC_OCC_PHANTOM_AVOIDANCE
        - 状況： occ の read phase で観測した masstree node の状態が、 commit phase において変化していたため、phantom problem を起こしている懸念がある。
        - 返せる情報:　 masstree node version に何回操作が割り込んだか（version 値の差分).
        - 備考：read phase 時に読み込んだ version と version を取得できる masstree node へのポインタがセットで保存されているだけなので、それ以外の情報を返すのは追加コストが発生する。
  - LTX
    - read
      - read upper bound violation
        - CC_LTX_READ_UPPER_BOUND_VIOLATION
          - 状況：自身が前置すると決定した LTX 群に対して、コミット時点で最終的な位置取りを計算した結果、前置しようとしてるエポックがこれまでに読み込みで観測したバージョンよりも古いため、自身の view を壊すことを検知してアボートする。
          - 返せる情報：
          - 備考：前置（エポックを確定的に変更する）行為はコミット時点で行う。読み込み操作時点では前置対象となりうる ltx id 群をまとめるだけ。従って、どの read 操作に起因したかは直接的には分からない。
      - read area violation
        - CC_LTX_READ_AREA_VIOLATION
          - 状況：トランザクション開始時に宣言した read area に違反する読み込み操作を行った。
          - 返せる情報：
    - write
      - committed read protection
        - CC_LTX_WRITE_COMMITTED_READ_PROTECTION
          - 状況：自身の write が committed read を壊してしまうため、自身をアボートする。
          - 返せる情報:　 Storage id. コミット済みの読み込み操作を侵害しえた自身の write 操作を試みた key string
      - phantom avoidance
        - CC_LTX_PHANTOM_AVOIDANCE
          - 状況：自身の write が committed range read を壊してしまうため、自身をアボートする。
          - 返せる情報：　 Storage id. 実行しようとした自身の insert / delete / upsert にまつわる key 情報。
- USER_ABORT
  - 状況: ユーザーによる shirakami::abort を実行したため。