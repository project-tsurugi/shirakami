
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

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class single_long_search_read_area_2_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "search-single_long_search_read_area_2_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(single_long_search_read_area_2_test, read_area_negative_hit) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(enter(s), Status::OK);
    ASSERT_EQ(tx_begin({s,
                        transaction_options::transaction_type::LONG,
                        {},
                        {{}, {st}}}),
              Status::OK);
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(search_key(s, st, "", vb), Status::ERR_READ_AREA_VIOLATION);
    ASSERT_EQ(leave(s), Status::OK);
}

TEST_F(single_long_search_read_area_2_test, // NOLINT
       read_area_empty_negative_not_hit) {  // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(enter(s), Status::OK);
    ASSERT_EQ(tx_begin({s,
                        transaction_options::transaction_type::LONG,
                        {},
                        {{}, {}}}),
              Status::OK);
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(search_key(s, st, "", vb), Status::WARN_NOT_FOUND);
    ASSERT_EQ(leave(s), Status::OK);
}

TEST_F(single_long_search_read_area_2_test,    // NOLINT
       read_area_not_empty_negative_not_hit) { // NOLINT
    Storage st{};
    Storage st2{};
    ASSERT_EQ(create_storage("1", st), Status::OK);
    ASSERT_EQ(create_storage("2", st2), Status::OK);
    Token s{};
    ASSERT_EQ(enter(s), Status::OK);
    ASSERT_EQ(tx_begin({s,
                        transaction_options::transaction_type::LONG,
                        {},
                        {{}, {st2}}}),
              Status::OK);
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(search_key(s, st, "", vb), Status::WARN_NOT_FOUND);
    ASSERT_EQ(leave(s), Status::OK);
}

} // namespace shirakami::testing