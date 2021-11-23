
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

class wp_init_fin_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-wp_basic-wp_init_fin_test");
        FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/wp_init_fin_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(wp_init_fin_test, init_fin) { // NOLINT
    ASSERT_EQ(Status::OK, wp::fin());
    ASSERT_EQ(Status::WARN_NOT_INIT, wp::fin());
    ASSERT_EQ(Status::OK, wp::init());
    ASSERT_EQ(Status::WARN_ALREADY_INIT, wp::init());
    ASSERT_EQ(Status::OK, wp::fin());
    ASSERT_EQ(Status::OK, wp::init());
}

} // namespace shirakami::testing
