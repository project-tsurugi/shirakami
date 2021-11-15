
#include <thread>
#include <xmmintrin.h>

#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/wp.h"

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
        FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/wp_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(wp_test, basic) { // NOLINT
#ifdef WP
#if WP_LEVEL == 0
    ASSERT_EQ(WP_LEVEL, 0);
#else
    ASSERT_EQ(true, true);
#endif
#else
    ASSERT_EQ(true, true);
#endif
}

TEST_F(wp_test, init_fin) { // NOLINT
    ASSERT_EQ(Status::OK, wp::fin());
    ASSERT_EQ(Status::WARN_NOT_INIT, wp::fin());
    ASSERT_EQ(Status::OK, wp::init());
    ASSERT_EQ(Status::WARN_ALREADY_INIT, wp::init());
    ASSERT_EQ(Status::OK, wp::fin());
    ASSERT_EQ(Status::OK, wp::init());
}

TEST_F(wp_test, wp_meta_register) { // NOLINT
    wp::wp_meta meta{};
    meta.register_wp(1, 1);
    ASSERT_EQ(meta.get_wped().size(), 1);
    auto rv = meta.get_wped();
    ASSERT_EQ(rv.at(0).first, 1);
    ASSERT_EQ(rv.at(0).second, 1);
    std::vector<std::pair<std::size_t, std::size_t>> batch_wps{{2, 2}, {3, 3}};
    meta.register_wp(batch_wps);
    ASSERT_EQ(meta.get_wped().size(), 3);
    rv = meta.get_wped();
    ASSERT_EQ(rv.at(1).first, 2);
    ASSERT_EQ(rv.at(1).second, 2);
    ASSERT_EQ(rv.at(2).first, 3);
    ASSERT_EQ(rv.at(2).second, 3);
}

TEST_F(wp_test, wp_regi_remove) { // NOLINT
    wp::wp_meta wp_info;
    std::atomic<std::size_t> fin_register{0};
    std::vector<std::thread> th_vc;
    th_vc.reserve(std::thread::hardware_concurrency());

    auto work = [&wp_info, &fin_register](std::size_t const id) {
        epoch::epoch_t ce{epoch::get_global_epoch()};
        Status rc{wp_info.register_wp(ce, id)};
        if (rc == Status::OK) {
            ++fin_register;
            ASSERT_EQ(wp_info.remove_wp(id), Status::OK);
        }
    };

    for (std::size_t i = 0; i < std::thread::hardware_concurrency(); ++i) {
        th_vc.emplace_back(work, i);
    }

    for (auto&& elem : th_vc) { elem.join(); }

    ASSERT_EQ(fin_register > 0, true);
}

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
    std::pair<wp::wp_meta**, std::size_t> wp_info =
            yakushima::get<wp::wp_meta*>(page_set_meta_storage_view,
                                         storage_view);
    ASSERT_NE(wp_info.first, nullptr);
    wp::wp_meta* wp_ptr = *wp_info.first;
    ASSERT_NE(wp_ptr, nullptr);
    ASSERT_EQ(wp_ptr->get_wped().size(), 0);
    wp_ptr->register_wp(1, 1);
    ASSERT_EQ(wp_ptr->get_wped().size(), 1);
    ASSERT_EQ(Status::OK, wp_ptr->remove_wp(1));
    ASSERT_EQ(wp_ptr->get_wped().size(), 0);
    wp_ptr->clear_wped();
}

} // namespace shirakami::testing
