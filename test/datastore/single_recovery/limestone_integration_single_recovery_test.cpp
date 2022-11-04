
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
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

#include "boost/filesystem.hpp"
#include "boost/foreach.hpp"

namespace shirakami::testing {

using namespace shirakami;

class limestone_integration_single_recovery_test
    : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-data_store-"
                                  "limestone_integration_single_recovery_test");
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

TEST_F(limestone_integration_single_recovery_test, // NOLINT
       one_page_write_one_storage) {               // NOLINT
    // prepare
    std::string log_dir{};
    log_dir = create_log_dir_name();
    LOG(INFO) << ">> first start up";
    init({database_options::open_mode::CREATE, log_dir}); // NOLINT
    LOG(INFO) << "<< first start up";

    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("1", st, {2, ""}));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, st, "a", "A"));
    ASSERT_EQ(Status::OK, upsert(s, st, "b", "B"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));

    LOG(INFO) << ">> first shutdown";
    fin(false);
    LOG(INFO) << "<< first shutdown";
    LOG(INFO) << ">> second start up";
    init({database_options::open_mode::RESTORE, log_dir}); // NOLINT
    LOG(INFO) << "<< second start up";

    // test: storage num
    std::vector<Storage> st_list{};
    ASSERT_EQ(Status::OK, storage::list_storage(st_list));
    ASSERT_EQ(st_list.size(), 1); // because single recovery
    ASSERT_EQ(Status::OK, enter(s));

    // test: contents
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s, st, "a", vb));
    ASSERT_EQ(vb, "A");
    ASSERT_EQ(Status::OK, search_key(s, st, "b", vb));
    ASSERT_EQ(vb, "B");
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
    LOG(INFO) << ">> second shutdown";
    fin(false);
    LOG(INFO) << "<< second shutdown";
}

} // namespace shirakami::testing