# write skew に関するアノーマリを検証するテスト

## 目的

* write skew アノーマリを検出し、適切に処理できるかテストする。

## テストファイル

* write_skew_test.cpp
  + read x, write y と read y, write x が重なるスケジューリングをテストしてる。
