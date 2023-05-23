
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string>
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
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

#include "boost/filesystem.hpp"
#include "boost/foreach.hpp"

namespace shirakami::testing {

using namespace shirakami;

// regression testcase to verify recover+storage_get_options+insert
class li_single_recovery_storage_get_set_options_insert_test
    : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-data_store-"
                                  "li_single_recovery_"
                                  "storage_get_set_options_insert_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google; // NOLINT
};

TEST_F(li_single_recovery_storage_get_set_options_insert_test, // NOLINT
       check_storage_operation_after_recovery) {               // NOLINT
    // start
    std::string log_dir{};
    int tid = syscall(SYS_gettid); // NOLINT
    std::uint64_t tsc = rdtsc();
    log_dir =
            "/tmp/shirakami-" + std::to_string(tid) + "-" + std::to_string(tsc);
    init({database_options::open_mode::CREATE, log_dir}); // NOLINT

    // prepare test data
    Storage st0{};
    ASSERT_EQ(Status::OK, create_storage("T0", st0));
    Storage st1{};
    ASSERT_EQ(Status::OK, create_storage("T1", st1));
    Storage st10{};
    ASSERT_EQ(Status::OK, create_storage("T10", st10));

    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, st0, "a", "A"));
    ASSERT_EQ(Status::OK, insert(s, st1, "a", "A"));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));

    // shut down
    fin(false);

    // recovery
    init({database_options::open_mode::RESTORE, log_dir}); // NOLINT

    storage_option options{};
    ASSERT_EQ(Status::OK, get_storage("T0", st0));
    ASSERT_EQ(Status::OK, storage_get_options(st0, options));
    // cleanup
    fin();
}

} // namespace shirakami::testing