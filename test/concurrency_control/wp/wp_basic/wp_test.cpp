
#include <xmmintrin.h>

#include <mutex>
#include <thread>

#include "concurrency_control/wp/include/wp.h"

#include "concurrency_control/include/tuple_local.h"

#if defined(RECOVERY)

#include "boost/filesystem.hpp"

#endif

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::testing {

class wp_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-wp_test");
        FLAGS_stderrthreshold = 0;        // output more than INFO
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/tmp/wp_test_log");
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(false, log_dir_); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
    static inline std::string log_dir_;        // NOLINT
};

TEST_F(wp_test, wp_meta_basic) { // NOLINT
    Storage storage{};
    ASSERT_EQ(Status::OK, register_storage(storage));
    std::string_view storage_view = {
            reinterpret_cast<char*>(&storage), // NOLINT
            sizeof(storage)};
    Storage page_set_meta_storage = wp::get_page_set_meta_storage();
    std::string_view page_set_meta_storage_view = {
            reinterpret_cast<char*>(&page_set_meta_storage), // NOLINT
            sizeof(page_set_meta_storage)};
    std::pair<wp::wp_meta**, std::size_t> out{};
    ASSERT_EQ(yakushima::status::OK,
              yakushima::get<wp::wp_meta*>(page_set_meta_storage_view,
                                           storage_view, out));
    ASSERT_NE(out.first, nullptr);
    wp::wp_meta* wp_ptr = *out.first;
    ASSERT_NE(wp_ptr, nullptr);
    ASSERT_EQ(wp::wp_meta::empty(wp_ptr->get_wped()), true);
    wp_ptr->register_wp(1, 1);
    ASSERT_EQ(wp::wp_meta::empty(wp_ptr->get_wped()), false);
    auto wps{wp_ptr->get_wped()};
    ASSERT_EQ(wps.at(0).first, 1);
    ASSERT_EQ(wps.at(0).second, 1);
    ASSERT_EQ(Status::OK, wp_ptr->remove_wp(1));
    ASSERT_EQ(wp::wp_meta::empty(wp_ptr->get_wped()), true);
    wps = wp_ptr->get_wped();
    ASSERT_EQ(wps.at(0).first, 0);
    ASSERT_EQ(wps.at(0).second, 0);
    wp_ptr->clear_wped();
}

} // namespace shirakami::testing
