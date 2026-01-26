
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

namespace shirakami::testing {

using namespace shirakami;

class li_single_recovery_storage_get_set_options_test
    : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-data_store-"
                                  "li_single_recovery_"
                                  "storage_get_set_options_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google; // NOLINT
};

TEST_F(li_single_recovery_storage_get_set_options_test, // NOLINT
       check_storage_operation_after_recovery) {        // NOLINT
    // start
    std::string log_dir{};
    int tid = syscall(SYS_gettid); // NOLINT
    std::uint64_t tsc = rdtsc();
    log_dir =
            "/tmp/shirakami-" + std::to_string(tid) + "-" + std::to_string(tsc);
    init({database_options::open_mode::CREATE, log_dir}); // NOLINT

    // prepare test data
    Storage st{};
    // not modified data
    ASSERT_EQ(Status::OK, create_storage("1", st, {2, "3"}));
    // modified data
    ASSERT_EQ(Status::OK, create_storage("4", st, {5, "6"}));
    ASSERT_EQ(Status::OK, storage_set_options(st, {7, "8"}));

    // shut down
    fin(false);

    // recovery
    init({database_options::open_mode::RESTORE, log_dir}); // NOLINT

    // shut down
    fin(false);

    // recovery
    init({database_options::open_mode::RESTORE, log_dir}); // NOLINT

    ASSERT_EQ(Status::OK, get_storage("1", st));
    storage_option options{};
    ASSERT_EQ(Status::OK, storage_get_options(st, options));
    // check recovery state about not modified options (*1)
    ASSERT_EQ(2, options.id());
    ASSERT_EQ("3", options.payload());
    // check recovery state about modified options (*2)
    ASSERT_EQ(Status::OK, get_storage("4", st));
    ASSERT_EQ(Status::OK, storage_get_options(st, options));
    ASSERT_EQ(7, options.id());
    ASSERT_EQ("8", options.payload());

    // cleanup
    fin();
}

} // namespace shirakami::testing
