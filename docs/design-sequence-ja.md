# Sequence design doc

## この文書について

* shirakami で利用可能なシーケンス機能に関して、APIとその仕様, 機能実現に際するストレージレイヤーとの連携をまとめる。

## 本プロジェクトにおけるシーケンスの仕様

* ある連番を生成するための機構。
* トランザクショナルではなく、 API 実行時にその操作が完遂される。

## Type

* `using SequenceId = std::size_t`
  + データベース上のシーケンスオブジェクトに対して一意に識別するための識別子に用いられる型

* `using SequenceValue = std::int64_t`
  + とある SequenceVersion に紐づいたシーケンスの値に用いられる型

* `using SequenceVersion = std::size_t`
  + 0 から始まり、単調増加していくシーケンスオブジェクトのバージョン値

## API と仕様

* `Status create_sequence(SequenceId* id, Token token = nullptr)`
  - 新しいシーケンスオブジェクトを生成する。その初期値は SequenceValue, 
  SequenceVersion が 0 となる。
  - token が nullptr でなかったとき、その token が指す実行情報領域を用いて logging 
  を行う。nullptr だったときは内部的に enter command を用いて空いている実行情報領域
  を探索し、それを用いて logging を行う。

* `Status update_sequence(Token token, SequenceId id, SequenceVersion version, SequenceValue value)`
  - id に紐づくシーケンスオブジェクトにおける SequenceVersion, SequenceValue を
  version, value の値で更新する。
  - SequenceVersion がシーケンスオブジェクト上で単調増加にならない値を指定されたとき、
  エラーを返す。
  - シーケンスオブジェクトに対する操作のロギングにおいて、 token の実行情報領域を用いる。

* `Status read_sequence(SequenceId id, SequenceVersion* version, SequenceValue* value, Token token = nullptr)`
  - id に紐づくシーケンスオブジェクトにおける永続化が完了した範囲内で最大の 
  SequenceVersion とそれに対応する SequenceValue を返却する。

* `Status delete_sequence(SequenceId id, Token token = nullptr)`
  - id に紐づくシーケンスオブジェクトを削除する。該当オブジェクトが存在しなければ
  エラーを返す。
  - token が nullptr でなかったとき、その token が指す実行情報領域を用いて logging 
  を行う。nullptr だったときは内部的に enter command を用いて空いている実行情報領域
  を探索し、それを用いて logging を行う。

## シーケンス機構におけるロギングの設計
シーケンス機構において、シーケンスオブジェクトに対する書き込み操作はロギングを行うことになる。
その際、与えられた Token 情報に該当する実行情報領域あるいは内部的に探索した当該領域において、
一時的にその内部バッファに対してログを書きこむ。
シーケンスオブジェクトに対する書き込み操作をした時点のエポックからグローバルエポックが変化した場合、
その操作に関するログがグループコミットの対象となる。
Token 情報が関連する実行情報領域は、それぞれがストレージレイヤとのチャネルを有し、それを
介して滞留されたログがストレージレイヤに書き込まれる。