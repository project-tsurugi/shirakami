
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "clock.h"
#include "storage.h"
#include "test_tool.h"
#include "tsc.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/lpwal.h"
#include "concurrency_control/include/record.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

#include "boost/filesystem.hpp"
#include "boost/foreach.hpp"

namespace shirakami::testing {

using namespace shirakami;

class li_single_recovery_before_delete_records_test
    : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-data_store-"
                "li_single_recovery_before_delete_records_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google; // NOLINT
};

std::string create_log_dir_name() {
    int tid = syscall(SYS_gettid); // NOLINT
    std::uint64_t tsc = rdtsc();
    return "/tmp/shirakami-" + std::to_string(tid) + "-" + std::to_string(tsc);
}

TEST_F(li_single_recovery_before_delete_records_test, // NOLINT
       stx_delete_after_recovery) {                   // NOLINT
    // prepare
    std::string log_dir{};
    log_dir = create_log_dir_name();
    init({database_options::open_mode::CREATE, log_dir}); // NOLINT

    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("1", st, {2, ""}));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, st, "a", "A"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));

    fin(false);
    init({database_options::open_mode::RESTORE, log_dir}); // NOLINT

    ASSERT_EQ(Status::OK, enter(s));
    std::string vb{};
    ASSERT_EQ(Status::OK, delete_record(s, st, "a"));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));

    fin(false);
}

TEST_F(li_single_recovery_before_delete_records_test, // NOLINT
       ltx_delete_after_recovery) {                   // NOLINT
    // prepare
    std::string log_dir{};
    log_dir = create_log_dir_name();
    init({database_options::open_mode::CREATE, log_dir}); // NOLINT

    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("1", st, {2, ""}));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, st, "a", "A"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));

    fin(false);
    init({database_options::open_mode::RESTORE, log_dir}); // NOLINT

    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(Status::OK, delete_record(s, st, "a"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));

    fin(false);
}

} // namespace shirakami::testing