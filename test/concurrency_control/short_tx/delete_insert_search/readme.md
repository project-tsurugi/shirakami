- repeat_insert_search_delete_test1
  - insert tx, search tx, delete tx を繰り返す。

- repeat_insert_search_delete_test2
  - insert tx, search-delete tx, を繰り返す。

- delete_insert_delete_search
  - 同一 tx による delete insert delete search

- concurrent_insert_search_tx_insert_delete_tx
  - insert search tx を tx1, insert delete tx を tx2 とおき、tx1: insert, 
  tx2: insert, tx3: delete, tx3: commit, (wait for unhook gc,) tx4: search