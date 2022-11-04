
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
class limestone_integration_single_recovery_storage_get_set_options_lost_test
    : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-data_store-"
                                  "limestone_integration_single_recovery_"
                                  "storage_get_set_options_lost_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google; // NOLINT
};

// regression scenario - storage option was not recovered correctly
// when storage_id_undefined is specified for storage option
TEST_F(limestone_integration_single_recovery_storage_get_set_options_lost_test, // NOLINT
       check_storage_operation_after_recovery) { // NOLINT
    // start
    std::string log_dir{};
    int tid = syscall(SYS_gettid); // NOLINT
    std::uint64_t tsc = rdtsc();
    log_dir =
            "/tmp/shirakami-" + std::to_string(tid) + "-" + std::to_string(tsc);
    init({database_options::open_mode::CREATE, log_dir}); // NOLINT

    // prepare test data
    Storage st00{};
    ASSERT_EQ(Status::OK, create_storage("s", st00));

    Storage st10{};
    ASSERT_EQ(Status::OK,
              create_storage("T", st10, {storage_id_undefined, "abc"}));

    // shut down
    fin(false);

    // recovery
    init({database_options::open_mode::RESTORE, log_dir}); // NOLINT

    {
        std::vector<std::string> storages{};
        ASSERT_EQ(Status::OK, list_storage(storages));
        ASSERT_EQ(2, storages.size());
    }

    Storage st100{};
    ASSERT_EQ(Status::OK, get_storage("s", st100));
    ASSERT_EQ(st00, st100);
    storage_option options100{};
    ASSERT_EQ(Status::OK, storage_get_options(st100, options100));
    ASSERT_EQ(storage_id_undefined, options100.id());
    ASSERT_TRUE(options100.payload().empty());

    Storage st101{};
    ASSERT_EQ(Status::OK, get_storage("T", st101));
    ASSERT_NE(st100, st101);
    ASSERT_EQ(st10, st101);
    storage_option options101{};
    ASSERT_EQ(Status::OK, storage_get_options(st101, options101));
    ASSERT_EQ(storage_id_undefined, options101.id());
    ASSERT_EQ("abc", options101.payload());

    // cleanup
    fin();
}

} // namespace shirakami::testing