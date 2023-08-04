
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

class tsurugi_issue152 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue152");
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

TEST_F(tsurugi_issue152, DISABLED_simple) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));

    ASSERT_EQ(Status::OK, upsert(s1, st, "a", ""));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    // test
    ASSERT_EQ(
            Status::OK,
            tx_begin({s1, transaction_options::transaction_type::LONG, {st}}));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    // do forwarding
    std::string buf{};
    ASSERT_EQ(Status::OK, search_key(s2, st, "a", buf));
    // If tx is read only that it is read write mode at tx begin, not need to wait.
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing