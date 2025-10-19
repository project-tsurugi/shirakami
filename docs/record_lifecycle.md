# shirakami record 

## 本文書について

shirakamiが管理するレコード(shirakami::Record)の使用方法や生存期間に関して記述する

## レコードが保持する情報

- tid_word 
  - locked/latest/absent等のステータスフラグ
    - commit時にレコードをロックするのに使用
    - latest/absentの組合せでレコードが「挿入中」か「削除中」を表す
  - transaction id (tid)
  - epoch (commit epoch)
- key string
- version の単方向リスト
  - 各versionがvalue stringを持つ
  - versionもtid_wordを持ち、recordが持つものと情報が少し重複している
  - upsert/update操作により既存のキーの値が変更された場合に新たなversionが追加される
- OCC/LTXにより、どのepoch/txによってreadされたかの情報

(key string/value stringはstd::stringで保持される)

## レコードのライフサイクル

### レコードの生成

- DB起動時にWALから復元される
  - shirakami::Recordオブジェクトが作成され、そのポインタをvalueとするkey/valueがyakushimaへputされる (hook)
    - この際のレコードには latest フラグが立っている
- insert/upsert要求によりレコードが作成される 
  - shirakami::Recordオブジェクトが作成され、そのポインタをvalueとするkey/valueがyakushimaへputされる (hook)
    - この際のレコードは latestとabsent フラグが立っている (placeholder)
  - write setへinsert/upsert操作が登録される(レコードへ関連付けられる)
  - トランザクションがコミットされたタイミングで absentフラグが外され latestフラグのみとなる

### レコードの消滅

- delete_record 操作によってwrite setへdelete操作が登録される
- そのトランザクションがcommitされると、レコードのtid_wordへabsentフラグが立てられる
  - この時点ではインデックスからは除去されない

- gcが定期的にyakushimaへ全走査をかけ、absentフラグが立っているレコードを検知
  - yakushimaのremoveを呼び出し、インデックスからキーに対応するレコードを除去する (unhook)
  - 除去されたレコードはgcの管理するコンテナへ移動され、gcが定期的にエポックを監視して安全に解放されるタイミングでdeleteされる

### レコードとトランザクションとの関係

雑多な情報がたくさんあるが、write set/read setに関して述べる。
これらはトランザクションが管理するデータ構造で、トランザクションのコミット時にレコードの整合性検証や更新処理を行うために使用される。

- write set
  - insert/update/upsert/delete_record 操作によって登録されたもの
  - 1つのエントリが 1 レコードへの参照とアクション種別を含む
  - コミット時の検証において、write setのレコード(インデックス上にhookされている)を参照して必要なロック・更新処理を行う

- read set
  - read 操作(search_key, scan) によって登録されたエントリ
  - 1つのエントリが 1 レコードへの参照とread時のtid_wordを含む
  - コミット時の検証において、インデックス上のレコードとread時のtid_wordを比較し、整合性を検証する

## 注意

LTX関連はOCCよりも詳細な情報を保持するが、本資料では詳述しない
