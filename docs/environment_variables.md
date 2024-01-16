# 参照する環境変数

## この文書について

Shirakami コードから参照する環境変数の説明

## 開発者向け動作フラグ

* `SHIRAKAMI_ENABLE_WAITING_BYPASS`
  * waiting bypass 実行に関するフラグ。未指定時には、waiting bypass を実行する。
  * `SHIRAKAMI_ENABLE_WAITING_BYPASS=0` とすると、waiting bypass を実行しなくなる。

* `SHIRAKAMI_WAITING_BYPASS_TO_ROOT`
  * waiting bypass 実行時の動作に関するフラグ。未指定時には root を追い越さない waiting bypass モードで実行する。
  * `SHIRAKAMI_WAITING_BYPASS_TO_ROOT=1` とすると root を追い越すモードで実行する。
