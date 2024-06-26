
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

class li_single_recovery_delete_one_storage_test
    : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-datastore-"
                                  "li_single_recovery_"
                                  "delete_one_storage_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google; // NOLINT
};

TEST_F(li_single_recovery_delete_one_storage_test, // NOLINT
       check_storage_operation_after_recovery) {   // NOLINT
    // start
    std::string log_dir{};
    int tid = syscall(SYS_gettid); // NOLINT
    std::uint64_t tsc = rdtsc();
    log_dir =
            "/tmp/shirakami-" + std::to_string(tid) + "-" + std::to_string(tsc);
    init({database_options::open_mode::CREATE, log_dir}); // NOLINT

    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("0", st));
    ASSERT_EQ(Status::OK, delete_storage(st));

    fin(false);

    // re-start
    init({database_options::open_mode::RESTORE, log_dir}); // NOLINT
    std::vector<Storage> st_list{};
    ASSERT_EQ(Status::OK, storage::list_storage(st_list));
    ASSERT_EQ(0, st_list.size()); // 1 is due to recovery

    fin();
}

} // namespace shirakami::testing
