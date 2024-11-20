# 参照する環境変数

## この文書について

Shirakami コードから参照する環境変数の説明

## 開発者向け動作フラグ

* `SHIRAKAMI_ENABLE_WAITING_BYPASS`
  * waiting bypass 実行に関するフラグ。実行するかしないかを選択する。
  * デフォルト動作は waiting bypass を実行しない。
    * 未指定時、空文字列指定時には、デフォルト動作をする。
    * `SHIRAKAMI_ENABLE_WAITING_BYPASS=0` とすると、waiting bypass を実行しない。
    * `SHIRAKAMI_ENABLE_WAITING_BYPASS=1` とすると、waiting bypass を実行する。

* `SHIRAKAMI_WAITING_BYPASS_TO_ROOT`
  * waiting bypass 実行時の root を追い越す動作モードに関するフラグ。root を追い越すか追い越さないかを選択する。
  * デフォルト動作は root を追い越さないモードで実行する。
    * 未指定時、空文字列指定時には、デフォルト動作をする。
    * `SHIRAKAMI_WAITING_BYPASS_TO_ROOT=0` とすると root を追い越さないモードで実行する。
    * `SHIRAKAMI_WAITING_BYPASS_TO_ROOT=1` とすると root を追い越すモードで実行する。

* `SHIRAKAMI_ENABLE_OCC_EPOCH_LOG_BUFFERING`
  * OCC での epoch 単位のトランザクションログバッファリング動作に関するフラグ。バッファするかしないかを選択する。
  * デフォルト動作はバッファしない。
    * 未指定時、空文字列指定時には、デフォルト動作をする。
    * `SHIRAKAMI_ENABLE_OCC_EPOCH_LOG_BUFFERING=0` とすると、バッファしない。
    * `SHIRAKAMI_ENABLE_OCC_EPOCH_LOG_BUFFERING=1` とすると、バッファする。

* `SHIRAKAMI_ALWAYS_LOG_FLUSH_IN_BG`
  * lpwal log flush に関するフラグ。すべて flush worker daemon に任せるかどうかを選択する。
  * デフォルト動作はすべて flush worker daemon に任せず、 `commit()` 中にも条件によって実行する。
    * 未指定時、空文字列指定時には、デフォルト動作をする。
    * `SHIRAKAMI_ALWAYS_LOG_FLUSH_IN_BG=0` とすると、すべて flush worker daemon に任せず、 `commit()` 中にも条件によって実行する。
    * `SHIRAKAMI_ALWAYS_LOG_FLUSH_IN_BG=1` とすると、すべて flush worker daemon に任せる。

* `SHIRAKAMI_LPWAL_WORKER_NUM`
  * lpwal flush worker daemon に関するフラグ。スレッド数を10進数値で指定する。
  * デフォルト値は1。
    * 未指定時、空文字列指定時には、デフォルト動作をする。
