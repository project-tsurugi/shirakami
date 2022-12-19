
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

class li_logging_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-data_store-"
                                  "li_logging_test");
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

TEST_F(li_logging_test,       // NOLINT
       check_wal_file_existence_and_extention) { // NOLINT
    // prepare test
    init({database_options::open_mode::CREATE}); // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // prepare data
    std::string k{"k"};
    ASSERT_EQ(Status::OK, upsert(s, st, k, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT // (*1)

    auto ep = epoch::get_global_epoch();
    // wait durable (*1) log
    while (ep > lpwal::get_durable_epoch()) { _mm_pause(); }

    // check wal file existence
    std::string log_dir_str{lpwal::get_log_dir()};
    boost::filesystem::path log_path{log_dir_str};
    // verify
    boost::uintmax_t size1 = dir_size(log_path);
    EXPECT_EQ(size1 != 0, true);

    ASSERT_EQ(Status::OK, upsert(s, st, k, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT // (*2)

    ep = epoch::get_global_epoch();
    // wait durable (*2) log
    while (ep > lpwal::get_durable_epoch()) { _mm_pause(); }

    // verify
    boost::uintmax_t size2 = dir_size(log_path);
    EXPECT_EQ(size1 != size2, true);

    // clean up test
    ASSERT_EQ(Status::OK, leave(s));
    fin(false);
}

} // namespace shirakami::testing