# 起動コマンドのパラメーター

shirakami は起動コマンド (init) にて起動し、その引数において起動方法の詳細やロギングに関する設定が可能である。

- database_options::open_mode_
  - これは起動方法を選択できるオプションである。オプションには CREATE, RESTORE, CREATE_OR_RESTORE の三種類が存在する。
    - CREATE: データベースレコードが何も存在しない状態で起動する。
    - RESTORE: 以前の init コマンドから fin コマンドあるいは障害によるダウンまでロギングを有効にしていて、後述する log_directory_path_ を以前ロギングに利用していたディレクトリのパスへ適切に設定がされている場合、ロギングされた一貫性のある状態に復元したのちに起動する。
    - CREATE_OR_RESTORE: 可能であれば RESTORE mode で起動する。そうでなければ CREATE mode で起動する。

- database_options::log_directory_path_
  - これは起動時にリカバリを試みる際、以前の起動時におけるロギングによって用いられていたログディレクトリを指定し、これからのロギングにもログディレクトリとして用いるためのものである。起動オプションにログディレクトリが指定されず、ロギングが有効な場合は shirakami-111-222 のような temporally directory を /tmp 下に作成してログディレクトリとして用いる。名前に用いられる数字はプロセスid や TimestampCounter を用いたものであり、同一テストプログラム内における前後するテスト同士、並行実行しているテストプログラムなどが名前の衝突を起こさないようにするためである。