# Sequence design doc

## この文書について

* shirakami で利用可能なシーケンス機能に関して、APIとその仕様, 機能実現に際する
ストレージレイヤーとの連携をまとめる。

## 本プロジェクトにおけるシーケンスの仕様概要

* ある連番を生成するための機構。
* シーケンスオブジェクトの生成・削除操作は、トランザクション操作とは無関係に実行・
永続化がされる。
更新操作は関数呼び出しに与えられたトランザクションハンドルのトランザクション実行に
紐づいて実行され、そのトランザクションが成功したときに限って実行・永続化される。
永続化は epoch-base group commit のロギングフレーム枠に則るため、操作終了から永続化
まで一定時間を要する。

## Type

* `using SequenceId = std::size_t`
  + データベース上のシーケンスオブジェクトに対して一意に識別するための識別子に
  用いられる型

* `using SequenceValue = std::int64_t`
  + とある SequenceVersion に紐づいたシーケンスの値に用いられる型

* `using SequenceVersion = std::size_t`
  + 0 から始まり、単調増加していくシーケンスオブジェクトのバージョン値

## API と仕様

* `Status create_sequence(SequenceId* id)`
  - 新しいシーケンスオブジェクトを生成する。その初期値は SequenceValue,
  SequenceVersion が 0 となる。新しいシーケンスオブジェクトの id は引数を介して
  出力される。

* `Status update_sequence(Token token, SequenceId id, SequenceVersion version, SequenceValue value)`
  - id に紐づくシーケンスオブジェクトにおける SequenceVersion, SequenceValue を
  version, value の値で更新する。
  - SequenceVersion がシーケンスオブジェクト上で単調増加にならない値を指定されたとき、
  エラーを返す。
  - シーケンスオブジェクトに対する操作のロギングにおいて、 token の実行情報領域を
  用いる。
  - 本操作は token に対して commit(token) コマンドが実行され、その結果が成功したとき
  に限り実行される。

* `Status read_sequence(SequenceId id, SequenceVersion* version, SequenceValue* value)`
  - id に紐づくシーケンスオブジェクトにおける永続化が完了した範囲内で最大の
  SequenceVersion とそれに対応する SequenceValue を返却する。

* `Status delete_sequence(SequenceId id)`
  - id に紐づくシーケンスオブジェクトを削除する。該当オブジェクトが存在しなければ
  エラーを返す。

## シーケンス機構におけるロギングの設計
シーケンス機構において、シーケンスオブジェクトに対する書き込み操作はロギングを行うこと
になる。
その際、与えられた Token 情報に該当する実行情報領域あるいは内部的に探索した当該領域
において、一時的にその内部バッファに対してログを書きこむ。
シーケンスオブジェクトに対する書き込み操作をした時点のエポックからグローバルエポックが
変化した場合、その操作に関するログがグループコミットの対象となる。
Token 情報が関連する実行情報領域は、それぞれがストレージレイヤとのチャネルを有し、
それを介して滞留されたログがストレージレイヤに書き込まれる。