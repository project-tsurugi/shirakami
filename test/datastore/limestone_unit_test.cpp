
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

    std::string_view get_data_log_dir() { return data_log_dir_; }

    std::string_view get_metadata_log_dir() { return metadata_log_dir_; }

    void set_data_log_dir(std::string_view ld) { data_log_dir_ = ld; }

    void set_metadata_log_dir(std::string_view ld) { metadata_log_dir_ = ld; }

private:
    static inline std::once_flag init_google;    // NOLINT
    static inline std::string data_log_dir_;     // NOLINT
    static inline std::string metadata_log_dir_; // NOLINT
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

TEST_F(limestone_unit_test, DISABLED_logging_and_recover) {
    // decide test dir name
    int tid = syscall(SYS_gettid);
    std::uint64_t tsc = rdtsc();
    std::string data_dir =
            "/tmp/shirakami" + std::to_string(tid) + "-" + std::to_string(tsc);
    std::string metadata_dir = data_dir + "m";
    boost::filesystem::path data_location(data_dir);
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
    set_data_log_dir(data_dir);
    set_metadata_log_dir(metadata_dir);
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
    std::string v{"v"};
    Storage st{2};                   // NOLINT
    lc->add_entry(st, k, v, {1, 0}); // (*1)
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
    std::string v2{"v2"};
    lc->add_entry(st, k2, v2, {2, 0}); // (*2)
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

    // start datastore
    datastore = std::make_unique<limestone::api::datastore>(
            limestone::api::configuration({data_location}, metadata_path));
    d_ptr = datastore.get();
    d_ptr->recover();
    d_ptr->ready();

    limestone::api::snapshot* ss{d_ptr->get_snapshot()};
    ASSERT_TRUE(ss->get_cursor().next()); // point first
    ASSERT_EQ(ss->get_cursor().storage(), st);
    std::string buf{};
    ss->get_cursor().key(buf);
    ASSERT_EQ(buf, "k");
    ss->get_cursor().value(buf);
    ASSERT_EQ(buf, "v");
    ASSERT_TRUE(ss->get_cursor().next()); // point second
    ASSERT_EQ(ss->get_cursor().storage(), st);
    ss->get_cursor().key(buf);
    ASSERT_EQ(buf, "k2");
    ss->get_cursor().value(buf);
    ASSERT_EQ(buf, "v2");
    ASSERT_FALSE(ss->get_cursor().next()); // point none
}

} // namespace shirakami::testing