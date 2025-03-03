
#include <array>
#include <mutex>

#include "test_tool.h"

#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class read_only_tx_begin_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "helper-read_only_tx_begin_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(read_only_tx_begin_test, tx_begin_read_only_and_wp) { // NOLINT
    Token s{};
    Storage st{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::WARN_ILLEGAL_OPERATION,
              tx_begin({s,
                        transaction_options::transaction_type::READ_ONLY,
                        {st}}));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(read_only_tx_begin_test, double_tx_begin) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    wait_epoch_update();
    ASSERT_EQ(Status::WARN_ALREADY_BEGIN,
              tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(read_only_tx_begin_test, tx_begin_SS_epoch) { // NOLINT
    Token s1{};
    Token s2{};
    Token s3{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, enter(s3));

    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::LONG}));
    LOG(INFO) << static_cast<session*>(s1)->get_valid_epoch();
    wait_epoch_update();
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::LONG}));
    LOG(INFO) << static_cast<session*>(s2)->get_valid_epoch();
    wait_epoch_update();
    ASSERT_EQ(Status::OK,
              tx_begin({s3, transaction_options::transaction_type::READ_ONLY}));
    LOG(INFO) << static_cast<session*>(s3)->get_valid_epoch();

    ASSERT_NE(static_cast<session*>(s1)->get_valid_epoch(),
              static_cast<session*>(s2)->get_valid_epoch());
    ASSERT_NE(static_cast<session*>(s2)->get_valid_epoch(),
              static_cast<session*>(s3)->get_valid_epoch());
    ASSERT_EQ(static_cast<session*>(s1)->get_valid_epoch(),
              static_cast<session*>(s3)->get_valid_epoch());

    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
    ASSERT_EQ(Status::OK, leave(s3));
}

TEST_F(read_only_tx_begin_test, clone_api) { // NOLINT
    Token s1{};
    Token s2{};
    ASSERT_OK(enter(s1));
    ASSERT_OK(enter(s2));

    // from not running
    ASSERT_NE(Status::OK, tx_clone(s2, s1));

    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::READ_ONLY}));
    wait_epoch_update();
    // normal; OK
    ASSERT_OK(tx_clone(s2, s1));
    wait_epoch_update();
    // s2 is already running
    ASSERT_EQ(Status::WARN_ALREADY_BEGIN, tx_clone(s2, s1));

    ASSERT_OK(commit(s1));
    ASSERT_OK(commit(s2));

    ASSERT_OK(leave(s2));
    ASSERT_OK(leave(s1));
}

TEST_F(read_only_tx_begin_test, clone_behavior) { // NOLINT
    Token s{};
    Token sr{};
    Token sc{};
    Token sn{};

    Storage st;
    ASSERT_OK(create_storage("", st));

    ASSERT_OK(enter(s));
    ASSERT_OK(enter(sr));
    ASSERT_OK(enter(sc));
    ASSERT_OK(enter(sn));

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(insert(s, st, "a", "1"));
    ASSERT_OK(commit(s));
    wait_epoch_update();

    ASSERT_OK(tx_begin({sr, transaction_options::transaction_type::READ_ONLY}));
    wait_epoch_update();
    std::string str;
    ASSERT_OK(search_key(sr, st, "a", str));
    ASSERT_EQ(str, "1");
    wait_epoch_update();

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(update(s, st, "a", "2")); // "1" -> "2"
    ASSERT_OK(commit(s));
    wait_epoch_update();

    ASSERT_OK(tx_begin({sn, transaction_options::transaction_type::READ_ONLY}));
    ASSERT_OK(tx_clone(sc, sr));
    wait_epoch_update();
    ASSERT_OK(search_key(sn, st, "a", str));
    ASSERT_EQ(str, "2");
    ASSERT_OK(search_key(sc, st, "a", str));
    ASSERT_EQ(str, "1");

    ASSERT_OK(commit(sr));
    wait_epoch_update();
    ASSERT_OK(search_key(sn, st, "a", str));
    ASSERT_EQ(str, "2");
    ASSERT_OK(search_key(sc, st, "a", str));
    ASSERT_EQ(str, "1");

    ASSERT_OK(commit(sn));
    ASSERT_OK(commit(sc));

    ASSERT_OK(leave(sn));
    ASSERT_OK(leave(sc));
    ASSERT_OK(leave(sr));
    ASSERT_OK(leave(s));
}

} // namespace shirakami::testing
