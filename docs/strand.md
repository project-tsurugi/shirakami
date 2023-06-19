# Strand: トランザクション内並列

- shirakami には strand 機構が存在する。これは単一トランザクションを複数スレッドによって並列処理可能な機構である。それによって、単一のトランザクション処理を効率よく実施する。

- strand の仕様
  - tx_begin コマンドが成功して以降、commit / abort コマンドを送信する前のデータアクセスAPIに関して、スレッドセーフになる。
    - データアクセスAPI: データの読み書きに関する物。open_scan, read_key_from_scan, read_value_from_scan, next, close_scan, search_key, update, upsert, insert, delete_record, 
  - データアクセスAPIと commit / abort コマンドの送信が前後してしまったときの動作は未定義。

- strand の注意点
  - strand スレッド間では単一トランザクションに属するという性質を有するため、データの同期によってキャッシュ汚染等が引き起こされて単一スレッドよりも性能が劣化するケースが多い。ユーザーは確実に strand の恩恵を受けられるような使い方を検討し、実施する必要がある。
  - 上記より、可能であれば単一Tx単一スレッドで実施することが望ましく、長大なTxを別Txへ分割可能な時、可能な限り分割した方が性能は良い。また、(フルスキャンの分割など)可能な限り限定的な利用にとどめた方が良い。

- strand のキャッシュ汚染や直列化による性能劣化となる要素
  - 各種 local read / write / node set への排他
  - 各種データアクセスAPIの正常終了・異常終了の排他
  
- strand が有効と思われるワークロード
  - データ分布に知識を有するときのフルスキャン
    - 例えば、データが自然数列で 1, 2, ..., 1,000,000 あるとする。このようなときに、strand A によって [1, 500,0000], strand B によって [500,000, 1,000,000] をそれぞれスキャンする。ACID の性質上、それらが成功し、コミット申請による結果も成功していたとき、このTxは [1, 1,000,000] のスキャンを実施したとことと等価になる。このように、長大な範囲をスキャンする際に同程度の仕事にそれを分割し、 strand スレッドに割り当てることが有意義である。


- 使用例
```
    Token token{};
    auto ret = enter(token);
    if (ret != Status::OK) {
        // 同時並行的にセッションを開きすぎたときの対応か、例外処理
    } // success enter

    // table create
    Storage st{};
    ret = create_storage("test strand", st);
    if (ret != Status::OK) {
        // 失敗用の処理
    }

    // insert to the table
    // tx begin;
    ret = tx_begin({token, transaction_options::transaction_type::SHORT});
    for (std::size_t i = 1; i < 1000000; ++i) {
        std::string key{"12345678"}; // 8 bytes key
        memcpy(key.data(), &size_t, sizeof(i)); // make key
        ret = insert(token, st, key, "value");
        if (ret != Status:OK) {
            // 例外処理
        }
    }
    ret = commit(token);
    if (ret != Status::OK) {
        // 例外処理
    }

    // use strand!
    ret = tx_begin({token, transaction_options::transaction_type::SHORT});
    auto strand = [token, st](bool is_a) {
        ScanHandle hd{};
        std::size_t key_l_value{};
        if (is_a) {
            key_l_value = 1;
        } else {
            key_l_value = 500000;
        }
        std::size_t key_r_value{};
        if (is_a) {
            key_r_value = 500000;
        } else {
            key_r_value = 1000000
        }
        std::string key_l{"12345678"}; // 8 bytes key
        std::string key_r{"12345678"}; // 8 bytes key
        memcpy(key_l.data(), &key_l_value, sizeof(key_l_value));
        memcpy(key_r.data(), &key_r_value, sizeof(key_r_value));
        ret = open_scan(token, st, key_l, scan_endpoint::INCLUSIVE, key_r, scan_endpoint::INCLUSIVE);
        if (ret != Status::OK) {
            // 例外処理
        }
        // read_key_from_scan / read_value_from_scan で読み、next でカーソルを次へ動かすことを繰り返して範囲を読み込む。
    }

    // async strand a
    std::thread strand_a(strand, true);
    // async strand b
    strand(false);

    // wait strand a
    strand_a.join();

    // commit
    ret = commit(token);
    if (ret != Status::OK) {
        // 例外処理
    }

    ret = leave(token);
    if (ret != Status::OK) {
        // 例外処理
    }
```
