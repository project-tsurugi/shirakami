
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"

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

class long_delete_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "long_delete_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/long_delete_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

void wait_change_epoch() {
    auto ce{epoch::get_global_epoch()};
    for (;;) {
        if (ce != epoch::get_global_epoch()) { break; }
        _mm_pause();
    }
}

TEST_F(long_delete_test, start_before_epoch) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    {
        std::unique_lock stop_epoch{epoch::get_ep_mtx()};
        ASSERT_EQ(Status::OK, tx_begin(s, false, true, {st}));
        ASSERT_EQ(Status::WARN_PREMATURE, delete_record(s, st, ""));
    }
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_delete_test, single_long_delete) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, tx_begin(s, false, true, {st}));
    wait_change_epoch();
    ASSERT_EQ(Status::OK, delete_record(s, st, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    wait_change_epoch();
    wait_change_epoch();
    // verify key existence
    Record* rec_ptr{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, get<Record>(st, "", rec_ptr));

    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_delete_test, delete_at_non_existing_storage) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    Storage st{};
    ASSERT_EQ(Status::OK, tx_begin(s, false, true, {}));
    wait_change_epoch();
    ASSERT_EQ(Status::WARN_WRITE_WITHOUT_WP, delete_record(s, st, ""));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
