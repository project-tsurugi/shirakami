
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

std::string create_log_dir_name() {
    int tid = syscall(SYS_gettid); // NOLINT
    std::uint64_t tsc = rdtsc();
    return "/tmp/shirakami-" + std::to_string(tid) + "-" + std::to_string(tsc);
}

TEST_F(li_logging_test,                          // NOLINT
       check_wal_file_existence_and_extention) { // NOLINT
    // prepare test
    std::string log_dir{};
    log_dir = create_log_dir_name();
    boost::filesystem::path data_location(log_dir);
    init({database_options::open_mode::CREATE, log_dir}); // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // prepare data
    for (std::size_t i = 0; i < 100; ++i) { // NOLINT
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, upsert(s, st, std::to_string(i), "v"));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT // (*1)
    }

    LOG(INFO) << "before first shut down: " << dir_size(data_location);
    fin(false);
    LOG(INFO) << "after first shut down: " << dir_size(data_location);

    // second start up
    init({database_options::open_mode::CREATE, log_dir}); // NOLINT
    LOG(INFO) << "after second start up: " << dir_size(data_location);
    fin(false);
    LOG(INFO) << "after second shut down: " << dir_size(data_location);
    // third start up
    init({database_options::open_mode::CREATE, log_dir}); // NOLINT
    LOG(INFO) << "after third start up: " << dir_size(data_location);
    fin(false);
    LOG(INFO) << "after third shut down: " << dir_size(data_location);
}

} // namespace shirakami::testing