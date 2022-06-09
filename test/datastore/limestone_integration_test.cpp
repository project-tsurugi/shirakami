
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

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

std::size_t dir_size(boost::filesystem::path path) {
    std::size_t total_file_size{0};
    BOOST_FOREACH (const boost::filesystem::path& p,
                   std::make_pair(boost::filesystem::directory_iterator(path),
                                  boost::filesystem::directory_iterator())) {
        if (!boost::filesystem::is_directory(p)) {
            total_file_size += boost::filesystem::file_size(p);
        }
    }

    return total_file_size;
}

TEST_F(limestone_integration_test,
       check_wal_file_existence_and_extention) { // NOLINT
    // prepare test
    init(false, "/tmp/shirakami"); // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, register_storage(st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    LOG(INFO) << epoch::get_durable_epoch();
    // prepare data
    std::string k{"k"};
    epoch::epoch_t target_epoch{};
    {
        std::unique_lock<std::mutex> lk{epoch::get_ep_mtx()};
        ASSERT_EQ(Status::OK, upsert(s, st, k, ""));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT // (*1)
        target_epoch = epoch::get_global_epoch();
    }

    LOG(INFO) << epoch::get_durable_epoch();
    // wait durable (*1) log
    for (;;) {
        if (epoch::get_durable_epoch() >= target_epoch) { break; }
        _mm_pause();
    }

    // check wal file existence
    std::string log_dir_str{lpwal::get_log_dir()};
    boost::filesystem::path log_path{log_dir_str};
    // verify
    boost::uintmax_t size1 = dir_size(log_path);
    ASSERT_EQ(size1 != 0, true);

    LOG(INFO) << epoch::get_durable_epoch();
    epoch::epoch_t target_epoch2{};
    {
        std::unique_lock<std::mutex> lk{epoch::get_ep_mtx()};
        ASSERT_EQ(Status::OK, upsert(s, st, k, ""));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT // (*2)
        target_epoch2 = epoch::get_global_epoch();
    }

    LOG(INFO) << epoch::get_durable_epoch();
    // wait durable (*2) log
    for (;;) {
        if (epoch::get_durable_epoch() >= target_epoch2) { break; }
        _mm_pause();
    }

    // verify
    boost::uintmax_t size2 = dir_size(log_path);
    ASSERT_EQ(size1 != size2, true);

    // clean up test
    ASSERT_EQ(Status::OK, leave(s));
    fin(false);
}

TEST_F(limestone_integration_test, check_recovery) { // NOLINT
    // start
    init(false, "/tmp/shirakami"); // NOLINT

    // storage creation
    Storage st{};
    ASSERT_EQ(Status::OK, register_storage(st));

    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    // data creation
    ASSERT_EQ(Status::OK, upsert(s, st, "", "")); // (*1)
    ASSERT_EQ(Status::OK, commit(s));             // NOLINT
    // want durable epoch
    auto want_de{epoch::get_global_epoch()};

    // wait durable for limestone
    for (;;) {
        if (want_de < epoch::get_durable_epoch()) { _mm_pause(); }
        break;
    }
    fin();

    // start
    init(true, "/tmp/shirakami"); // NOLINT

    // test: log exist
    std::string vb{};
    ASSERT_EQ(Status::OK, enter(s));
    // test: check recovery
    ASSERT_EQ(Status::OK, search_key(s, st, "", vb));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));

    fin();
}

} // namespace shirakami::testing
