
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

class limestone_integration_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-data_store-"
                                  "limestone_integration_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google; // NOLINT
};

std::size_t dir_size(boost::filesystem::path& path) {
    std::size_t total_file_size{0};
    BOOST_FOREACH (const boost::filesystem::path& p, // NOLINT
                   std::make_pair(boost::filesystem::directory_iterator(path),
                                  boost::filesystem::directory_iterator())) {
        if (!boost::filesystem::is_directory(p)) {
            total_file_size += boost::filesystem::file_size(p);
        }
    }

    return total_file_size;
}

TEST_F(limestone_integration_test, check_persistent_call_back) { // NOLINT
    init();
    for (;;) {
        sleepMs(PARAM_EPOCH_TIME);
        if (epoch::get_durable_epoch() > 20) { break; } // NOLINT
    }
    fin();
}

TEST_F(limestone_integration_test,               // NOLINT
       check_wal_file_existence_and_extention) { // NOLINT
    // prepare test
    init(false, "/tmp/shirakami"); // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, register_storage(st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // prepare data
    std::string k{"k"};
    ASSERT_EQ(Status::OK, upsert(s, st, k, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT // (*1)

    // wait durable (*1) log
    sleep(1); // not strictly

    // check wal file existence
    std::string log_dir_str{lpwal::get_log_dir()};
    boost::filesystem::path log_path{log_dir_str};
    // verify
    boost::uintmax_t size1 = dir_size(log_path);
    EXPECT_EQ(size1 != 0, true);

    ASSERT_EQ(Status::OK, upsert(s, st, k, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT // (*2)

    // wait durable (*2) log
    sleep(1); // not strictly

    // verify
    boost::uintmax_t size2 = dir_size(log_path);
    EXPECT_EQ(size1 != size2, true);

    // clean up test
    ASSERT_EQ(Status::OK, leave(s));
    fin(false);
}
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

void recovery_test() {
    // start
    std::string log_dir{};
    int tid = syscall(SYS_gettid); // NOLINT
    std::uint64_t tsc = rdtsc();
    log_dir =
            "/tmp/shirakami-" + std::to_string(tid) + "-" + std::to_string(tsc);
    init(false, log_dir); // NOLINT

    // storage creation
    register_storage_and_upsert_one_record();
    sleep(1);

    fin(false);

    // start
    init(true, log_dir); // NOLINT

    // test: log exist
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    // test: check recovery
    std::string vb{};
    std::vector<Storage> st_list{};
    ASSERT_EQ(Status::OK, list_storage(st_list));
    for (auto&& st : st_list) {
        ASSERT_EQ(Status::OK, search_key(s, st, "", vb));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    }
    ASSERT_EQ(Status::OK, leave(s));
    fin();
}

TEST_F(limestone_integration_test, check_recovery) { // NOLINT
    ASSERT_NO_FATAL_FAILURE(recovery_test());        // NOLINT
}

} // namespace shirakami::testing