
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
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

#include "limestone/api/datastore.h"

namespace shirakami::testing {

using namespace shirakami;

class limestone_unit_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-data_store-"
                                  "limestone_unit_test");
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

TEST_F(limestone_unit_test, simple) {
    // decide test dir name
    int tid = syscall(SYS_gettid);
    std::uint64_t tsc = rdtsc();
    std::string test_dir =
            "/tmp/shirakami" + std::to_string(tid) + "-" + std::to_string(tsc);
    std::string metadata_dir = test_dir + "m";
    boost::filesystem::path data_location(test_dir);
    boost::filesystem::path metadata_path(metadata_dir);

    // prepare durable epoch
    std::atomic<size_t> limestone_durable_epoch{0};
    auto set_limestone_durable_epoch =
            [&limestone_durable_epoch](std::size_t n) {
                limestone_durable_epoch.store(n, std::memory_order_release);
            };
    auto get_limestone_durable_epoch = [&limestone_durable_epoch]() {
        return limestone_durable_epoch.load(std::memory_order_acquire);
    };

    // prepare data / metadata directory
    if (boost::filesystem::exists(data_location)) {
        boost::filesystem::remove_all(data_location);
    }
    ASSERT_TRUE(boost::filesystem::create_directory(data_location));
    if (boost::filesystem::exists(metadata_dir)) {
        boost::filesystem::remove_all(metadata_dir);
    }
    ASSERT_TRUE(boost::filesystem::create_directory(metadata_dir));

    // allocate datastore
    std::unique_ptr<limestone::api::datastore> datastore;
    datastore = std::make_unique<limestone::api::datastore>(
            limestone::api::configuration({data_location}, metadata_path));
    limestone::api::datastore* d_ptr{datastore.get()};
    d_ptr->add_persistent_callback(set_limestone_durable_epoch);

    //create log_channel
    limestone::api::log_channel* lc{&d_ptr->create_channel(data_location)};

    // start datastore
    d_ptr->ready();

    // switch epoch for initialization
    d_ptr->switch_epoch(1);

    // flush logs
    lc->begin_session();
    std::string k{"k"};
    lc->add_entry(2, k, "value", {1, 0}); // (*1)
    lc->end_session();

    // change new epoch
    d_ptr->switch_epoch(2);

    // wait for durable
    for (;;) {
        if (get_limestone_durable_epoch() >= 1) { break; }
        _mm_pause();
    }

    // check existence of log file about (*1)
    ASSERT_TRUE(boost::filesystem::exists(data_location));

    // log file size after flushing log (*1)
    boost::uintmax_t size1 = dir_size(data_location);

    // flush logs
    lc->begin_session();
    std::string k2{"k2"};
    lc->add_entry(2, k2, "value", {2, 0}); // (*2)
    lc->end_session();

    // change new epoch
    d_ptr->switch_epoch(3);

    // wait for durable
    for (;;) {
        if (get_limestone_durable_epoch() >= 2) { break; }
        _mm_pause();
    }

    // log file size after flushing log (*2)
    boost::uintmax_t size2 = dir_size(data_location);

    // verify size1 != size2
    ASSERT_NE(size1, size2);

    // clean up
    d_ptr->shutdown();
}

} // namespace shirakami::testing
