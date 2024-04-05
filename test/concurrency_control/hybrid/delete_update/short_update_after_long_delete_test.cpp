
#include <xmmintrin.h>

#include <bitset>
#include <future>
#include <mutex>

#include "concurrency_control/include/epoch.h"

#include "shirakami/interface.h"

#include "test_tool.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class short_update_after_long_delete : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-hybrid-delete_update-"
                "short_update_after_long_delete_test");
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

TEST_F(short_update_after_long_delete, independent_tx) { // NOLINT
    Storage st{};
    create_storage("", st);
    std::string k("k"); // NOLINT
    std::string v("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, delete_record(s, st, k));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::WARN_NOT_FOUND, update(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
