
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

class wp_remove_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-wp_basic-wp_remove_test");
        FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/wp_remove_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(wp_remove_test, wp_regi_remove) { // NOLINT
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

} // namespace shirakami::testing
