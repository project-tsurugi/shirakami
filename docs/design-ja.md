# Shirakami design doc

## この文書について

* Tx エンジン Shirakami の概要
* 現状の ASIS を示したものであり、適宜更新する。

## 仕様

* Shirakami はトランザクション実行エンジンであり、トランザクション実行を可能とした KVS である。
* ストレージを複数制御することが可能であり、ストレージ間でキー空間は分かれている。
* キー /バリューは可変長のバイナリ列であり、その型は Shirakami において関知しない。
* キー 一つに対して、バリューを一つ持つ。
  + キー:バリュー を N:1 にしたければ、一つのオブジェクトのディープコピーやシャローコピーを必要に応じてそれぞれのキーに付帯させて挿入すればよい。
  + キー:バリュー を 1: N にしたければ、バリューはバイナリ列として N 個のオブジェクトを連結させればよい。
* 全ての操作は明示的なキーを介して行う。

## design concept

* 最大並行トランザクション数はビルド時のオプションによって決める。
  + それを前提にすることで、ロックフリー技術によって性能向上を実現した。
* 並行性制御法は Shirakami を用いる。 Shirakami とはショートトランザクション向けに Silo ベース論理、ロングトランザクション向けに Yatsumine 論理を用いた、特定のロングトランザクションを優遇しつつショートトランザクション性能を落とさないことを実現した新しい並行性制御法である。
* ログ永続化法は並行 WAL を行い、データストア Limestone を用いる。
* In-memory index の構造には Masstree をベースとした Yakushima を用いる。

## Shirakami 起動・終了 API

* `init`
  + Shirakami engine を起動する。起動オプションとしてリカバリの実施有無やログの所在を指定可能。
* `fin`
  + Shirakami engine を終了する。終了オプションとして、現在保持するインメモリログを永続化してから終了するか、それを待たずに終了するかを指定可能。

## ストレージ操作 API

* `create_storage`
  + ストレージを作成する。ストレージ キー としてバイナリ文字列を指定することが可能である。出力引数で内部的に割り当てられた識別情報（Storage) が呼び出し側へ与えられる。呼び出し側は該当ストレージへのトランザクション・ストレージ操作は与えられた Storage を用いて行う。
* `get_storage`
  + `create_storage` でストレージ キー を指定した場合、その時割り当てられた Storage を再取得（確認）する。
* `exist_storage`
  + Storage が存在するか確認する。
* `delete_storage`
  + Storage を削除する。
* `list_storage`
  + 過去に作成され、これまで削除されていないストレージの一覧を確認する。

## セッション操作 API

* `enter`
  + 新しいセッションを獲得する。このセッションを介してトランザクション処理を行う。
* `leave`
  + 獲得したセッションを返却する。返却後のセッションを介してトランザクション処理を行ってはならない。高性能化のために領域の再利用を行っているため、意図しない挙動になる。

## トランザクション操作 API

### 開始操作
* `tx_begin`
  + トランザクション開始を明示的に宣言する。トランザクションのモードをショート・ロング・読み込みのみを選択できる。本操作を実施せずにデータ操作を実施した場合、ショートトランザクションとして実行される。

### データ操作

* 読み込み
  + 単一読み込み
    - `search_key`
      - 指定された キー と対応するエントリを確認する。
    - `exist_key`
      - 指定された キー と対応するエントリが存在するかを確認する。
  + 範囲読み込み
    - `open_scan`
      - 範囲読み込みの準備を行う。内部的には指定範囲内における キー と対応するエントリを計算し、キャッシュする。カーソルは辞書順において最も若いものを指している。
    - `next`
      - `open_scan` で獲得したカーソルを一つ進める。
    - `read_key_from_scan`
      - カーソルが指しているエントリのキーを読み込む。
    - `read_value_from_scan`
      - カーソルが指しているエントリのバリューを読み込む。
    - `scannable_total_index_size`
      - ある `open_scan` でキャッシュしたエントリ数を確認する。
    - `close_scan`
      - `open_scan` で獲得したキャッシュを破棄する。
* 書き込み
  + `insert`
    - 指定された キー と対応するエントリを挿入する。指定された キー が実行時において存在しないことを前提とする。
  + `upsert`
    - 指定された キー を指定されたバリューで更新する。この操作実行時において、 キーの存在・非存在は関知しない。
  + `delete_record`
    - 指定された キー に関するエントリを削除する。操作実行時において キー は存在することを前提とする。
  + `update`
    - 指定された キー に関して指定されたバリューで更新する。この操作実行時のいて、 キー は存在することを前提とする。

### 終了操作

* `commit`
  + トランザクションが成功することを期待して、処理中であったトランザクションの終了を宣言し、成功可能かの検証を実施する。
* `abort`
  + 処理中であったトランザクションの内容を棄却する。

## 永続化確認 API

* `check_commit`
  + デフォルトでは CC のみの commit で制御が返る。その際に得られた情報を用いて、そのトランザクションが永続化されたか確認する API である。

## クリーンアップ API

* `delete_record`
  + 全てのストレージ、ストレージ内に格納されたキーバリューを削除する。

## シーケンス API

* `create_sequence`
  + シーケンスを作成する。
* `update_sequence`
  + シーケンスを更新する。
* `delete_sequence`
  + シーケンスを削除する。

## トランザクションステータス API

* `acquire_tx_state_handle`
  + 処理中のトランザクションに関するステータスハンドルを取得する。
* `release_tx_state_handle`
  + 獲得したステータスハンドルを返却する。
* `tx_check`
  + 獲得したステータスハンドルに関するトランザクションの状態を確認する。

## ロギングコールバック API

* `database_set_logging_callback`
  + ロギングの際に呼び出したいコールバック関数を登録する。

## データストア API

* `get_datastore`
  + 利用しているデータストアへの参照を獲得する。