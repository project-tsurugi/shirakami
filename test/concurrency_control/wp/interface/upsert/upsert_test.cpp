
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/ongoing_tx.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/version.h"

#include "concurrency_control/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class upsert_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-upsert_test");
        FLAGS_stderrthreshold = 0;
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/build/upsert_test_log");
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(false, log_dir_); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google; // NOLINT
    static inline std::string log_dir_;       // NOLINT
};

TEST_F(upsert_test, occ_simple) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::string k{"k"};
    std::string v{"v"};
    ASSERT_EQ(upsert(s, st, k, v), Status::OK);
    ASSERT_EQ(Status::OK, commit(s));

    // verify result
    Record* rec{};
    ASSERT_EQ(Status::OK, get<Record>(st, k, rec));
    ASSERT_NE(rec, nullptr);
    std::string vb{};
    rec->get_latest()->get_value(vb);
    ASSERT_EQ(vb, v);
    std::string kb{};
    rec->get_key(kb);
    ASSERT_EQ(kb, k);

    ASSERT_EQ(Status::OK, leave(s));
}
TEST_F(upsert_test, bt_simple) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::string k{"k"};
    std::string v{"v"};
    ASSERT_EQ(tx_begin(s, false, true, {st}), Status::OK);

    auto wait_epoch_update = []() {
        epoch::epoch_t ce{epoch::get_global_epoch()};
        for (;;) {
            if (ce == epoch::get_global_epoch()) {
                _mm_pause();
            } else {
                break;
            }
        }
    };
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

    ASSERT_EQ(tx_begin(s, false, true, {st}), Status::OK);
    wait_epoch_update();
    ASSERT_EQ(upsert(s, st, k, v), Status::OK);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
