
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/record.h"
#include "concurrency_control/include/version.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_upsert_mv_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-long_upsert_mv_test");
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

TEST_F(long_upsert_mv_test, new_epoch_new_version) { // NOLINT
    Token s{};
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    ASSERT_EQ(Status::OK, enter(s));
    std::string k{"K"};
    std::string first_v{"v"};
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    ASSERT_EQ(upsert(s, st, k, first_v), Status::OK);
    ASSERT_EQ(commit(s), Status::OK);
    wait_epoch_update();
    std::string second_v{"v2"};
    // Writing after the epoch has changed should be the new version.
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    ASSERT_EQ(upsert(s, st, k, second_v), Status::OK);
    ASSERT_EQ(commit(s), Status::OK);
    Record* rec_ptr{};
    ASSERT_EQ(Status::OK, get<Record>(st, k, rec_ptr));
    version* ver{rec_ptr->get_latest()};
    ASSERT_NE(ver, nullptr);
    std::string vb{};
    ver->get_value(vb);
    ASSERT_EQ(vb, second_v);
    ver = ver->get_next();
    ASSERT_NE(ver, nullptr);
    ver->get_value(vb);
    ASSERT_EQ(vb, first_v);
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
