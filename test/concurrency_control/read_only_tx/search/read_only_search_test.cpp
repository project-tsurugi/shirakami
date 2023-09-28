
#include <array>
#include <mutex>

#include "test_tool.h"

#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class read_only_search_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-read_only_tx-search-"
                "read_only_search_test");
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

TEST_F(read_only_search_test, user_abort) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::string buf{};
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, abort(s));
    auto tri = transaction_result_info(s);
    ASSERT_EQ((*tri).get_reason_code(), reason_code::USER_ABORT);
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(read_only_search_test, operation_before_after_start_epoch) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::string buf{};
    stop_epoch();
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    // operation before start epoch
    ASSERT_EQ(Status::WARN_PREMATURE, search_key(s, st, "", buf));
    resume_epoch();
    wait_epoch_update();
    // operation after start epoch
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, st, "", buf));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(read_only_search_test, ltx_write_rtx_read) { // NOLINT
    // =================================
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));

    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "", "v1"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, upsert(s, st, "", "v2"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    auto ce = epoch::get_global_epoch();
    for (;;) {
        if (ce >= epoch::get_cc_safe_ss_epoch()) {
            _mm_pause();
            continue;
        }
        break;
    }
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    wait_epoch_update();
    std::string buf{};
    // =================================
    // test phase
    ASSERT_EQ(Status::OK, search_key(s, st, "", buf));
    ASSERT_EQ(buf, "v2");
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    // =================================

    // =================================
    // verify phase
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s, st, "", buf));
    ASSERT_EQ(buf, "v2");
    // =================================

    // =================================
    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(read_only_search_test, search_SS_version) { // NOLINT
    // =================================
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));

    Token s{};
    Token s1{};
    Token s2{};
    Token s3{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, enter(s3));

    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "", "v1"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::LONG}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "", "v2"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::LONG}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "", "v3"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s3, transaction_options::transaction_type::READ_ONLY}));
    wait_epoch_update();
    // =================================

    // =================================
    // test phase
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    std::string buf{};
    ASSERT_EQ(Status::OK, search_key(s1, st, "", buf));
    ASSERT_EQ(buf, "v1");
    ASSERT_EQ(Status::OK, search_key(s2, st, "", buf));
    ASSERT_EQ(buf, "v2");
    ASSERT_EQ(Status::OK, search_key(s3, st, "", buf));
    ASSERT_EQ(buf, "v1"); // read SS
    // =================================

    // =================================
    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
    ASSERT_EQ(Status::OK, leave(s3));
}

TEST_F(read_only_search_test, very_long_key) { // NOLINT
                                               // prepare
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::string sb{};
    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::READ_ONLY}),
              Status::OK);
    wait_epoch_update();
    // test
    ASSERT_EQ(
            Status::WARN_INVALID_KEY_LENGTH,
            search_key(
                    s, st,
                    std::string(static_cast<std::basic_string<char>::size_type>(
                                        1024 * 36),
                                'a'),
                    sb));

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing