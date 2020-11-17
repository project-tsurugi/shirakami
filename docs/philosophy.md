# Philosophy
2020-11-17 tanabe

## この文書について
shirakami のアルゴリズム構成と，その強みとなるワークロード・弱みとなるワークロードをまとめる．

## Shirakami のターゲットビルド構成
- Overall : low contention, short transaction 向けのアーキテクチャである．
- 索引構造 : yakushima (based on Masstree)
  - Strong points : concurrent put/get/delete, low (r/w) contention workloads, epoch based parallel resource management. <br >
    put によって引き起こされる競合ポイントはボーダーノード．多数のレコード（挿入）によって多数のボーダーノードが存在する場合，競合しにくい．
    write (put/delete) でしかロックを取らず，読み込み処理は楽観的にアトミック操作．従って high concurrency. 単一セッション内で読み込んだ
    値が並行して削除され，再度その読み込んだ値にアクセスしてメモリアクセス違反を起こさないように，セッションという枠組みでそれを保護するとともに，
    削除処理によって発生した garbage node を並行して解放可能な仕組みがある．
  - Weak points : scan, high (r/w) contention workloads. <br>
  タイムスタンプ（バージョンフィールド）の不変によってアトミック read 操作を保証している．この楽観的技術は low contention workloads に
  おいて（悲観技法と比較してロッキングによってキャッシュを汚さないために）高い読み込み性能を発揮する．read はタイムスタンプ不変のタイミングを
  高速につかみ取りたいが，put/delete 過多なワークロードではその楽観検証に失敗するリスクが増加する．また，scan operation はボーダーノード
  のファンアウトサイズ分 (key-)value をアトミックに読み込む必要があるが，ロックの粒度はノードであるため，read 同様に書き込み過多なワークロ
  ードでは楽観検証に失敗するリスクが増加する．
  - 想定議論
    - high concurrency ? : yes. <br>
    ロックの粒度が小さい．ボーダーノードのファンアウトを小さくすれば一定レコード数に対してより多くのボーダーノード（競合ポイント）となり，並行
    性は上がるが，この場合ノード分割に伴いメモリ確保(ヒープ競合），メモリ使用量増加等も伴う．そういったことのうまくバランスが取れた（とオリジナ
    ル論文著者らが定めた）設定がファンアウト 16.
- 並行性制御 : based on Silo
  - Strong points : low (r/w) contention workloads, short transaction, no centralized structure (without epoch), 
  read only snap(?), optimistic technique, epoch based parallel resource management. <br>
  read は楽観的技術を用いるため，上記同様低競合環境にて高い性能を発揮する．索引構造 yakushima はアトミック操作であるが，Silo はトランザク
  ショナル操作であることと，楽観的技術が用いられていることから，ショートトランザクションで高い性能を発揮する．順序調停に関するメタデータテーブル
  を持たず，中央構造は eopch カウンタのみであり，これの変更は（論文著者らの設定では） 40 ms とキャッシュ汚染影響を受けないようにすることがで
  きる．従ってキャッシュに親和的である．エポック単位でリソース管理をバッチ処理する．
  - Weak points : high (r/w) contention workloads), long transaction(ex. scan). <br>
  楽観的技術を用いるため，高競合環境に不利である．また，transaction life time が伸びることは楽観的技術に不利に働くため，ロングトランザクシ
  ョンも苦手である．
- ロギング : based on Concurrent Prefix Recovery
  - Strong points : high throughput (against wal), holding 1 copy, parallel creating copy(?), silo's read only snap 統合
  の可能性. <br>
  wal と比較して，（チェックポインティングフェーズに入らなければ）ロギングにまつわる処理が何もないため high throughput (pre-commit/s) で
  ある．Zigzag, Ping-Pong と比べて，保有するコピー数が 1 と少なくて省メモリである．チェックポイントフェーズにおいて，チェックポインターが到
  達する前のレコードにアクセスしたワーカーはコピー作成を肩代わりすることができる．これはチェックポイント時間短縮と pre-commit/s 低下の影響が
  あり，何を優先するかで肩代わりするしないは（設計あるいは将来的なビルドオプションとして）選択可能である．CPR copy と silo's read only 
  snap shot は統合可能な期待が強い．
  - Weak points : latency due to snapshot approach. <br>
  チェックポイントを作成し終えるまで durable にならず，そのレイテンシは小さくない．差分チェックポイント技術を適応してチェックポイント作成時間
  を短縮した場合，差分チェックポイントをいかにマージするか，マージのために何らかのロジックの quiescent time はあるか，それでもレイテンシが
  総合的に短縮となるか，という検討が必要となる．