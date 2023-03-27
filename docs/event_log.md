# shirakami におけるイベントログに関して

- 本資料の目的
  - shirakami で GLOG_v=35 相当で扱われるイベントログに関して、そのヘッダー名、出力形式、出力内容を説明する。

- イベントログ一覧
  - header: `/:shirakami:timing:start_wait`
    - 出力形式：ヘッダー＋ tx id
    - 出力内容：LTX モードのトランザクションがコミット待ちに関する検証の結果、待たなければいけないことが判明したこと。

  - header: `/:shirakami:timing:start_verify`
    - 出力形式：ヘッダー＋ tx id
    - 出力内容：LTX モードのトランザクションが（他のTXに待たされることなく）コミット成否の検証を開始する前であること。

  - header: `/:shirakami:timing:start_abort`
    - 出力形式：ヘッダー＋ tx id
    - 出力内容：LTX モードのトランザクションがコミット成否の検証に失敗し、自身のアボート処理を実施する前であること。

  - header: `/:shirakami:timing:precommit_with_nowait`
    - 出力形式：ヘッダー＋ tx id
    - 出力内容：LTX モードのトランザクションがコミット成否の検証に成功し、検証は他のトランザクションに待たされなかった。

  - header: `/:shirakami:timing:precommit_with_wait`
    - 出力形式：ヘッダー＋ tx id
    - 出力内容：LTX モードのトランザクションがコミット成否の検証に成功し、検証は他のトランザクションに待たされていた。

  - header: `/:shirakami:timing:start_register_read_by`
    - 出力形式：ヘッダー＋ tx id
    - 出力内容：LTX モードのトランザクションがコミット成否の検証に成功し、読み込み情報を記録する前であること。

  - header: `/:shirakami:timing:start_expose_local_write`
    - 出力形式：ヘッダー＋ tx id
    - 出力内容：LTX モードのトランザクションがコミット成否の検証に成功し、書き込み情報をグローバルへ反映する前であること。

  - header: `/:shirakami:timing:start_process_sequence`
    - 出力形式：ヘッダー＋ tx id
    - 出力内容：LTX モードのトランザクションがコミット成否の検証に成功し、シーケンスに関する書き込み情報をグローバルへ反映する前であること。

  - header: `/:shirakami:timing:start_process_logging`
    - 出力形式：ヘッダー＋ tx id
    - 出力内容：LTX モードのトランザクションがコミット成否の検証に成功し、データストアサービスへインメモリにログを送付する前であること。
