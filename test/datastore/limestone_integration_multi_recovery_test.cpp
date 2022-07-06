
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "clock.h"
#include "test_tool.h"
#include "tsc.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/lpwal.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

#include "boost/filesystem.hpp"
#include "boost/foreach.hpp"

namespace shirakami::testing {

using namespace shirakami;

class limestone_integration_multi_recovery_test
    : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-data_store-"
                                  "limestone_integration_multi_recovery_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google; // NOLINT
};

void register_storage_and_upsert_one_record() {
    // prepare
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    Storage st{};
    ASSERT_EQ(Status::OK, register_storage(st));

    ASSERT_EQ(Status::OK, enter(s));
    // data creation
    ASSERT_EQ(Status::OK, upsert(s, st, "", "")); // (*1)
    ASSERT_EQ(Status::OK, commit(s));             // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

void recovery_test(std::size_t recovery_num) {
    // start
    std::string log_dir{};
    int tid = syscall(SYS_gettid); // NOLINT
    std::uint64_t tsc = rdtsc();
    log_dir =
            "/tmp/shirakami-" + std::to_string(tid) + "-" + std::to_string(tsc);
    init(false, log_dir); // NOLINT

    std::vector<Storage> st_list{};
    for (std::size_t i = 0; i < recovery_num; ++i) {
        register_storage_and_upsert_one_record();
        fin(false);
        // recovery
        init(true, log_dir); // NOLINT

        // verify
        ASSERT_EQ(Status::OK, list_storage(st_list));
        ASSERT_EQ(i + 1, st_list.size());
    }

    // test
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    // test: check recovery
    std::string vb{};
    ASSERT_EQ(Status::OK, list_storage(st_list));
    ASSERT_EQ(recovery_num, st_list.size());
    for (auto&& st : st_list) {
        ASSERT_EQ(Status::OK, search_key(s, st, "", vb));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    }
    ASSERT_EQ(Status::OK, leave(s));
    fin();
}

TEST_F(limestone_integration_multi_recovery_test,
       DISABLED_two_recovery_test) {                    // NOLINT
    ASSERT_NO_FATAL_FAILURE(recovery_test(2)); // NOLINT
}

} // namespace shirakami::testing