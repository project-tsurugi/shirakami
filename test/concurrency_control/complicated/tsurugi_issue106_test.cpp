
#include <glog/logging.h>

#include <mutex>

#include "clock.h"

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class tsurugi_issue106 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue106");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }
    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

/**
 * Issue 106 caused by single thread.
 */

TEST_F(tsurugi_issue106, simple1) { // NOLINT
    /**
     * Tx1. delete all. scan and delete but the workload is known as no record
     * exist.
     * 
     * Tx2. Insert some key twice. The second see already exist and do user abort.
     * 
     * Tx3. full scan and commit.
     */
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("test", st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // if we execute heavy test, you change this i.
    for (std::size_t i = 0; i < 1; ++i) { // NOLINT
        // Tx1.
        ScanHandle hd{};
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::SHORT}));
        if (Status::WARN_NOT_FOUND != open_scan(s, st, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, hd)) {
            // read all record
            for (;;) {
                std::string key_buf{};
                ASSERT_EQ(Status::WARN_ALREADY_DELETE,
                          read_key_from_scan(s, hd, key_buf));
                if (next(s, hd) == Status::WARN_SCAN_LIMIT) { break; }
            }

            // node set must not be empty
            auto* ti = static_cast<session*>(s);
            ASSERT_EQ(ti->get_node_set().empty(), false);
        }
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT

        // Tx2.
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::SHORT}));
        for (std::size_t i = 0; i < 100; ++i) { // NOLINT
            std::string k(1, i);
            ASSERT_EQ(Status::OK, insert(s, st, k, "v"));
        }

        // second
        for (std::size_t i = 0; i < 100; ++i) { // NOLINT
            std::string k(1, i);
            ASSERT_EQ(Status::WARN_ALREADY_EXISTS, insert(s, st, k, "v"));
        }

        // user abort
        ASSERT_EQ(Status::OK, abort(s));

        // Tx3. full scan
        // record empty state
        for (std::size_t i = 1;; ++i) {
            ASSERT_EQ(Status::OK,
                      tx_begin({s,
                                transaction_options::transaction_type::SHORT}));
            if (i % 20 == 0) { // NOLINT
                // to reduce log
                LOG(INFO) << i;
            }
            if (Status::WARN_NOT_FOUND != open_scan(s, st, "",
                                                    scan_endpoint::INF, "",
                                                    scan_endpoint::INF, hd)) {
                // read all record
                for (;;) {
                    std::string key_buf{};
                    ASSERT_EQ(Status::WARN_ALREADY_DELETE,
                              read_key_from_scan(s, hd, key_buf));
                    if (next(s, hd) == Status::WARN_SCAN_LIMIT) { break; }
                }

                // node set must not be empty
                auto* ti = static_cast<session*>(s);
                ASSERT_EQ(ti->get_node_set().empty(), false);
            }
            // expect Status::OK
            auto rc = commit(s);
            if (rc == Status::OK) { break; }
        }
    }

    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(tsurugi_issue106, 20230308_comment_tanabe) { // NOLINT
    // 自分で自分の挿入中レコードコンバートタイムスタンプを観測しないかどうか

    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("test", st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // test
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, st, "a", "v"));

    // stop gc to stop epoch
    {
        std::unique_lock<std::mutex> eplk{epoch::get_ep_mtx()};
        ASSERT_EQ(Status::OK, abort(s));
        ScanHandle hd{};
        // it must read deleted record
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::WARN_NOT_FOUND,
                  open_scan(s, st, "", scan_endpoint::INF, "",
                            scan_endpoint::INF, hd));
        auto* ti = static_cast<session*>(s);
        ASSERT_EQ(ti->get_read_set_for_stx().size(), 1);
        // insert and read timestamp is converted by it.
        ASSERT_EQ(Status::OK, insert(s, st, "a", "v"));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    }

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(tsurugi_issue106, 20230310_comment_tanabe) { // NOLINT
    /**
     * シナリオ：key a, c が存在する。それを自分でスキャンした後、 bbbbbbbbb (b x 9) を挿入する。
     * 現状のアーキテクチャでは挿入したときに自己ファントムの検知は末端のボーダーノードしか返却しないため、
     * a, c をスキャンしたボーダーノードと自身の挿入を整合する機会が無く、コミット時に node verify 成功してしまう。
     * なぜなら b9 の挿入を見逃してしまうから。
     */
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("test", st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, st, "a", ""));
    ASSERT_EQ(Status::OK, insert(s, st, "c", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string buf{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, buf));
    ASSERT_EQ(buf, "a");
    ASSERT_EQ(Status::OK, next(s, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, buf));
    ASSERT_EQ(buf, "c");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));
    // index scan include node version without read body.
    ASSERT_EQ(Status::OK, insert(s, st, std::string(9, 'b'), ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(tsurugi_issue106, 20230327_comment_tanabe) { // NOLINT
    /**
     * シナリオ：key a, c が存在する。それをtx1 がスキャンしたあと、 tx2 が bbbbbbbbb (b x 9) を挿入する。
     * tx1 はそれによってファントムを検知するはずである。
     */
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("test", st));
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s1, st, "a", ""));
    ASSERT_EQ(Status::OK, insert(s1, st, "c", ""));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    // test
    // tx 1 read a, c
    ScanHandle hd{};
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, open_scan(s1, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string buf{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s1, hd, buf));
    ASSERT_EQ(buf, "a");
    ASSERT_EQ(Status::OK, next(s1, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s1, hd, buf));
    ASSERT_EQ(buf, "c");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s1, hd));
    // index scan include node version without read body.
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s2, st, std::string(9, 'b'), ""));
    ASSERT_EQ(Status::OK, commit(s2));     // NOLINT
    ASSERT_EQ(Status::ERR_CC, commit(s1)); // NOLINT
    auto* ti = static_cast<session*>(s1);
    ASSERT_EQ(ti->get_result_info().get_reason_code(),
              reason_code::CC_OCC_PHANTOM_AVOIDANCE);

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(tsurugi_issue106, 20230328_insight_tanabe) { // NOLINT
    /**
     * シナリオ：key ax9, cx9 が存在する。それをtx1 がスキャンしたあと、 tx2 が ax8 + b を挿入する。
     * tx1 はそれによってファントムを検知するはずである。
     */
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("test", st));
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s1, st, std::string(9, 'a'), ""));
    ASSERT_EQ(Status::OK, insert(s1, st, std::string(9, 'c'), ""));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    // test
    // tx 1 read a, c
    ScanHandle hd{};
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, open_scan(s1, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string buf{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s1, hd, buf));
    ASSERT_EQ(buf, std::string(9, 'a'));
    ASSERT_EQ(Status::OK, next(s1, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s1, hd, buf));
    ASSERT_EQ(buf, std::string(9, 'c'));
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s1, hd));
    // index scan include node version without read body.
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s2, st, std::string(8, 'a') + "b", ""));
    ASSERT_EQ(Status::OK, commit(s2));     // NOLINT
    ASSERT_EQ(Status::ERR_CC, commit(s1)); // NOLINT
    auto* ti = static_cast<session*>(s1);
    ASSERT_EQ(ti->get_result_info().get_reason_code(),
              reason_code::CC_OCC_PHANTOM_AVOIDANCE);

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing