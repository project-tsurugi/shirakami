
#include <xmmintrin.h>
#include <thread>

#include "concurrency_control/silo/include/epoch.h"
#include "concurrency_control/wp/include/wp.h"

#if defined(RECOVERY)

#include "boost/filesystem.hpp"

#endif

#include "shirakami/interface.h"

#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class wp_test : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/wp_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
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

TEST_F(wp_test, wp_regi_remove) { // NOLINT
    wp::wp_meta wp_info;
    std::atomic<bool> ready_register{false};
    std::atomic<std::size_t> fin_register{0};
    std::atomic<bool> ready_remove{false};
    std::vector<std::thread> th_vc;
    th_vc.reserve(std::thread::hardware_concurrency());

    auto work = [&wp_info, &ready_register, &fin_register, &ready_remove](std::size_t id) {
        while (!ready_register.load(std::memory_order_acquire)) _mm_pause();
        epoch::epoch_t ce{epoch::get_global_epoch()};
        wp_info.register_wp(ce, id);
        ++fin_register;
        while (!ready_remove.load(std::memory_order_acquire)) _mm_pause();
        ASSERT_EQ(wp_info.remove_wp(id), Status::OK);
    };

    for (std::size_t i = 0; i < std::thread::hardware_concurrency(); ++i) {
        th_vc.emplace_back(work, i);
    }
    ready_register.store(true, std::memory_order_release);

    while (fin_register.load(std::memory_order_acquire) != std::thread::hardware_concurrency()) _mm_pause();
    ASSERT_EQ(wp_info.size_wp(), std::thread::hardware_concurrency());
    ready_remove.store(true, std::memory_order_acquire);

    for (auto&& elem : th_vc) {
        elem.join();
    }
}

TEST_F(wp_test, init_fin) { // NOLINT
    ASSERT_EQ(Status::OK, wp::fin());
    ASSERT_EQ(Status::WARN_NOT_INIT, wp::fin());
    ASSERT_EQ(Status::OK, wp::init());
    ASSERT_EQ(Status::WARN_ALREADY_INIT, wp::init());
    ASSERT_EQ(Status::OK, wp::fin());
}

} // namespace shirakami::testing
