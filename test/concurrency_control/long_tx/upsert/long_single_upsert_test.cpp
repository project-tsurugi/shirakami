
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/version.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class single_long_upsert_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "single_long_upsert_test");
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

TEST_F(single_long_upsert_test, start_before_epoch) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    {
        std::unique_lock stop_epoch{epoch::get_ep_mtx()}; // stop epoch
        ASSERT_EQ(Status::OK, tx_begin(s, TX_TYPE::LONG, {st}));
        ASSERT_EQ(Status::WARN_PREMATURE, upsert(s, st, "", ""));
    } // start epoch
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(single_long_upsert_test, long_simple) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::string k{"k"};
    std::string v{"v"};
    ASSERT_EQ(tx_begin(s, TX_TYPE::LONG, {st}), Status::OK);

    wait_epoch_update();
    ASSERT_EQ(upsert(s, st, k, v), Status::OK);

    // check internal record existing
    auto check_internal_record_exist = [k](Storage st) {
        Record* rec{};
        ASSERT_EQ(Status::OK, get<Record>(st, k, rec));
        ASSERT_NE(rec, nullptr);
    };

    check_internal_record_exist(st);
    ASSERT_EQ(abort(s), Status::OK);
    // after abort, exist with deleted state.
    check_internal_record_exist(st);

    ASSERT_EQ(tx_begin(s, TX_TYPE::LONG, {st}), Status::OK);
    wait_epoch_update();
    ASSERT_EQ(upsert(s, st, k, v), Status::OK);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing