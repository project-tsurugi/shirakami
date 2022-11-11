# complicated tests

- DBの上位レイヤーによる検証をサポートするための複雑なテスト
- comp_test.cpp
  - test 1
    - https://github.com/project-tsurugi/tsurugi-issues/issues/86
    - 上記イシューの検証をサポートするためのテスト。
  - test 2
    - 全てLTX. insert 3 件コミット。二番目の挿入したものを削除してコミット。削除したものと同じものを挿入してコミット。再度削除してコミット。スキャンを実行すると２件で意図通り。search を実行すると delete 済みのものが見える。