
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

class wp_register_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-wp_basic-wp_register_test");
        FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/wp_register_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(wp_register_test, wp_meta_register) { // NOLINT
    wp::wp_meta meta{};
    meta.register_wp(1, 1);
    ASSERT_EQ(meta.get_wped().size(), 1);
    auto rv = meta.get_wped();
    ASSERT_EQ(rv.at(0).first, 1);
    ASSERT_EQ(rv.at(0).second, 1);
}

} // namespace shirakami::testing
