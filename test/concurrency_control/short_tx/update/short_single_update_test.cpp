
#include <mutex>

#include "concurrency_control/include/record.h"

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
    ASSERT_EQ(Status::OK, tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    ASSERT_EQ(Status::WARN_ILLEGAL_OPERATION, update(s, {}, "", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
