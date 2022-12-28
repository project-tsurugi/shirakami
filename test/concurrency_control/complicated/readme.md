# complicated tests

- DBの上位レイヤーによる検証をサポートするための複雑なテスト
- comp_test.cpp
  - test 1
    - https://github.com/project-tsurugi/tsurugi-issues/issues/86
    - 上記イシューの検証をサポートするためのテスト。
  - test 2
    - 全てLTX. insert 3 件コミット。二番目の挿入したものを削除してコミット。削除したものと同じものを挿入してコミット。再度削除してコミット。スキャンを実行すると２件で意図通り。search を実行すると delete 済みのものが見える。

- tsurugi_issue148.cpp
  - 以下の処理を行うスレッドを３０並列で実行する。
    - delete from test2 where  key1 = :key1
      - key1は主キーの一部なのでrange scanになる
      - open_scanでrangeにヒットしたものに対してdelete_recordを実行
    - select foo, bar, baz from test
      - test表のストレージに対するfull scan
    - insert into test2 (key1,key2,key3,value1) values(:key1,:key2,:key3,:value1)
      - test2表のストレージに対するinsertになる
    - transactionのcommit