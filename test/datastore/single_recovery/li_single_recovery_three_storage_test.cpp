
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

class li_single_recovery_three_storage_test
    : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-data_store-"
                "li_single_recovery_three_storage_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google; // NOLINT
};

void create_storage_and_upsert_one_record(std::size_t const i) {
    // prepare
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    Storage st{};
    ASSERT_EQ(Status::OK, create_storage(std::to_string(i), st));
    ASSERT_EQ(Status::OK, enter(s));
    // data creation
    ASSERT_EQ(Status::OK, upsert(s, st, "", "")); // (*1)
    ASSERT_EQ(Status::OK, commit(s));             // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

void storage_operation_test(std::size_t storage_num) {
    // start
    std::string log_dir{};
    int tid = syscall(SYS_gettid); // NOLINT
    std::uint64_t tsc = rdtsc();
    log_dir =
            "/tmp/shirakami-" + std::to_string(tid) + "-" + std::to_string(tsc);
    init({database_options::open_mode::CREATE, log_dir}); // NOLINT

    for (std::size_t i = 0; i < storage_num; ++i) {
        create_storage_and_upsert_one_record(i);
    }
    sleep(1);

    fin(false);

    // re-start
    init({database_options::open_mode::RESTORE, log_dir}); // NOLINT
    std::vector<Storage> st_list{};
    ASSERT_EQ(Status::OK, storage::list_storage(st_list));
    ASSERT_EQ(storage_num, st_list.size()); // 1 is due to recovery

    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    // test: check recovery
    std::sort(st_list.begin(), st_list.end());
    std::size_t itr_num{0};
    Storage max_st{0};
    for (auto&& st : st_list) {
        ASSERT_EQ(st, (storage::initial_strg_ctr + itr_num) << 32); // NOLINT
        std::string vb{};
        ASSERT_EQ(Status::OK, search_key(s, st, "", vb));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
        ++itr_num;
        if (st > max_st) { max_st = st; }
    }
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    ASSERT_EQ(st >> 32, (max_st >> 32) + 1); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
    fin();
}

TEST_F(li_single_recovery_three_storage_test, // NOLINT
       check_storage_operation_after_recovery) {                 // NOLINT
    ASSERT_NO_FATAL_FAILURE(storage_operation_test(3));          // NOLINT
}

} // namespace shirakami::testing