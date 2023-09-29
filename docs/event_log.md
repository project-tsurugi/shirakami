# shirakami におけるイベントログに関して

- 本資料の目的
  - shirakami で GLOG_v=35 相当で扱われるイベントログに関して、そのヘッダー名、出力形式、出力内容を説明する。

- イベントログ一覧
  - shut down (shirakami::fin)
    - header: `/:shirakami:timing:shutdown:start_bg_commit`
    - header: `/:shirakami:timing:shutdown:end_bg_commit`
      - 出力内容：バックグラウンドコミットに関する終了処理の開始と終了。
  
    - header: `/:shirakami:timing:shutdown:start_send_txlog_wait_durable`
    - header: `/:shirakami:timing:shutdown:end_send_txlog_wait_durable`
      - 出力内容：データストアへインメモリログを送付し、そのログがデータストア内において永続的になることを待つ処理の開始と終了。
  
    - header: `/:shirakami:timing:shutdown:start_gc`
    - header: `/:shirakami:timing:shutdown:end_gc`
      - 出力内容：ガーベッジコレクションに関するバックグラウンドスレッドへ終了するシグナルを送信し、当該スレッドは特定のGCに関するルーティンを終えた後に残存するヒープメモリに関するガーベッジをメモリ解放する処理の開始と終了。
  
    - header: `/:shirakami:timing:shutdown:start_shutdown_datastore`
    - header: `/:shirakami:timing:shutdown:end_shutdown_datastore`
      - 出力内容：データストアをシャットダウンする処理の開始と終了。
  
    - header: `/:shirakami:timing:shutdown:start_cleanup_logdir`
    - header: `/:shirakami:timing:shutdown:end_cleanup_logdir`
      - 出力内容：ログディレクトリ以下を全て削除する処理の開始と終了。これは shirakami 起動時にログディレクトリの所在を指定されなかったときに有効である。
  
    - header: `/:shirakami:timing:shutdown:start_delete_all_records`
    - header: `/:shirakami:timing:shutdown:end_delete_all_records`
      - 出力内容：全てのストレージにおける全てのレコード(インデックスに格納されたバリューエントリ)に関して、インデックス構造からそれをアンフックし、そのメモリ領域を解放する処理の開始と終了。
  
    - header: `/:shirakami:timing:shutdown:start_shutdown_yakushima`
    - header: `/:shirakami:timing:shutdown:end_shutdown_yakushima`
      - 出力内容：yakushima に関するシャットダウン処理の開始と終了。当該処理は yakushima におけるツリー構造の全てを解体し、それに関するヒープメモリを解放する作業である。
  
<!-- - header: `/:shirakami:timing:shutdown:start_shutdown_thread_pool`
    - header: `/:shirakami:timing:shutdown:end_shutdown_thread_pool`
      - 出力内容：スレッドプールに関するシャットダウン処理の開始と終了。当該処理はスレッドプールのワーカースレッドへ終了シグナルを送信し、スレッドプールのタスクキューに格納された全てのタスクの終了とワーカースレッドの終了を行う。
-->

  - commit phase
    - header: `/:shirakami:timing:start_wait`
      - 出力形式：ヘッダー＋ tx id
      - 出力内容：LTX モードのトランザクションがコミット待ちに関する検証の結果、待たなければいけないことが判明したこと。
  
    - header: `/:shirakami:timing:start_verify`
      - 出力形式：ヘッダー＋ tx id
      - 出力内容：LTX モードのトランザクションが（他のTXに待たされることなく）コミット成否の検証を開始する前であること。
  
    - header: `/:shirakami:timing:start_abort`
      - 出力形式：ヘッダー＋ tx id
      - 出力内容：LTX モードのトランザクションがコミット成否の検証に失敗し、自身のアボート処理を実施する前であること。
  
    - header: `/:shirakami:timing:end_abort`
      - 出力形式：ヘッダー＋ tx id
      - 出力内容：LTX モードのトランザクションがコミット成否の検証に失敗し、自身のアボート処理を実施した後であること。
  
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

    - header: `/:shirakami:timing:end_precommit`
      - 出力形式：ヘッダー＋ tx id
      - 出力内容：LTX モードのトランザクションがコミット成否の検証に成功し、自身のコミット処理を実施した後であること。
  