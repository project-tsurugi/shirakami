# LTX waiting 調査のためのイベントログ

2023-10-18 (draft) ban

## 目的

LTX が waiting となった際にその原因調査に利用するためのイベントログについて説明する.

## 共通事項

* ログのロケーションプレフィックス (以下 LP) は `/:shirakami:wait_reason:` あるいは `/:shirakami:diag317:` から始まる文字列を用いる.
* ログレベル (`GLOG_v`) は 36 あるいは 38 で出力される.
    * これは暫定的な設定であり, 利便性を検討の後 35, 37, 40, 50 のいずれかのレベルに調整する予定である.
* session を特定する情報を出力する際には, ltx_id を出力する. TID-xxxx 形式の id を安全に取得できる状況では, その形式の id も並べて出力する.

## 一覧

* boundary check による待ち
    * LP `/:shirakami:wait_reason:boundary`, レベル 36
        * ある session のコミットが, boundary check により待たされたことを示す.
          待たされた session および待つ原因となった session を特定する情報 (ltx_id) が出力される.
* read area check による待ち
    * LP `/:shirakami:wait_reason:read_area`, レベル 36
        * ある session のコミットが, read area の check により待たされたことを示す.
          待たされた session および待つ原因となった session を特定する情報 (ltx_id) と
          対象の storage (テーブル) を特定する情報が出力される.
* premature による待ち
    * LP `/:shirakami:wait_reason:premature`, レベル 36
        * LTX の開始条件を満たさないうちに `commit()` を呼ばれたことを示す.
          shirakami の API 規約を守っていない場合に出力されうる.
          現在の global epoch と LTX が開始される epoch が出力される.
* global epoch 進捗
    * LP `/:shirakami:diag317:epoch`, レベル 38
        * global epoch が増加したことを示す.
          新しい epoch の値と、算出された cc safe snapshot の値を出力する.
* waiting LTX の一覧
    * LP `/:shirakami:diag317:bg_commit`, レベル 38
        * epoch 毎に 待たされている LTX の一覧 (各 ltx_id) 情報が出力される.

## 情報の利用方法

出力される情報をもとに調査を進めるための, 利用ガイドを示す.

TBD
