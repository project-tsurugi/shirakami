
#include <glog/logging.h>

#include <mutex>

#include "clock.h"
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class long_diagnostic_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "diagnostic-long_diagnostic_test");
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

TEST_F(long_diagnostic_test, simple) { // NOLINT
    // prepare storage
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));

    // prepare data
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    print_diagnostics(std::cout);

    // set lower counter higher than 9 to look hex string
    for (std::size_t i = 0; i < 10; ++i) {
        ASSERT_EQ(Status::OK, upsert(s, st, "k", "v"));
        ASSERT_EQ(Status::OK, commit(s));
    }

    // prepare situation
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    print_diagnostics(std::cout);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing