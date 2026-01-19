
#include <mutex>
#include <vector>

#include "clock.h"

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class tsurugi_issue176_3 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue176_3");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(tsurugi_issue176_3, comment_by_ban_20230228_1730) { // NOLINT
    // create storage
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));

    // prepare token
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // prepare initial record
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, st, "1", ""));
    ASSERT_EQ(Status::OK, insert(s, st, "2", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // delete tx
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, delete_record(s, st, "2"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    auto* ti = static_cast<session*>(s);
    auto deleted_epoch = ti->get_mrc_tid().get_epoch();

    // wait the record is target of gc
    while (!(deleted_epoch < garbage::get_min_begin_epoch() &&
             deleted_epoch < garbage::get_min_batch_epoch())) {
        _mm_pause();
    }

    // start full scan tx
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    // expecting reference 2 and crash
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(tsurugi_issue176_3, comment_by_tanabe_20230301_1643) { // NOLINT
    { GTEST_SKIP() << "LONG is not supported"; }
    // create storage
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));

    // prepare token
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));

    // test
    std::string key{"key"};
    std::string v1{"1"};
    std::string v2{"2"};
    ASSERT_EQ(
            Status::OK,
            tx_begin({s1, transaction_options::transaction_type::LONG, {st}}));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    // Tx1 insert key a value 1
    // tx2 insert key a value 2
    ASSERT_EQ(Status::OK, insert(s1, st, key, v1));
    ASSERT_EQ(Status::OK, insert(s2, st, key, v2));

    // asynchronous commit / abort
    std::atomic<std::size_t> prepare_num{0};
    auto commit_1 = [s1, &prepare_num]() {
        ++prepare_num;
        while (prepare_num.load(std::memory_order_acquire) != 2) {
            _mm_pause();
        }
        ASSERT_EQ(Status::OK, commit(s1));
    };
    auto abort_2 = [s2, &prepare_num]() {
        ++prepare_num;
        while (prepare_num.load(std::memory_order_acquire) != 2) {
            _mm_pause();
        }
        sleep(1);
        ASSERT_EQ(Status::OK, abort(s2));
    };

    std::thread th_1 = std::thread(commit_1);
    std::thread th_2 = std::thread(abort_2);

    th_1.join();
    th_2.join();

    // check value is valid
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    std::string buf{};
    ASSERT_EQ(Status::OK, search_key(s1, st, key, buf));
    ASSERT_EQ(buf, v1);

    // delete the record
    ASSERT_EQ(Status::OK, delete_record(s1, st, key));
    ASSERT_EQ(Status::OK, commit(s1));

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing
