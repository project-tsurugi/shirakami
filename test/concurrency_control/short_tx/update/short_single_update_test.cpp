
#include <mutex>

#include "concurrency_control/include/record.h"
#include "concurrency_control/include/session.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class simple_update : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-common-"
                                  "update-update_basic_test");
    }
    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(simple_update, read_only_mode_update) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    ASSERT_EQ(Status::WARN_ILLEGAL_OPERATION, update(s, {}, "", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_update, find_wp) { // NOLINT
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    Storage st{};
    create_storage("", st);
    ASSERT_EQ(
            Status::OK,
            tx_begin({s1, transaction_options::transaction_type::LONG, {st}}));
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::SHORT}));

    ASSERT_EQ(Status::ERR_CC, update(s2, st, "", ""));
    ASSERT_EQ(static_cast<session*>(s2)->get_result_info().get_reason_code(),
              reason_code::CC_OCC_WP_VERIFY);

    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}
} // namespace shirakami::testing
