
#include <xmmintrin.h>

#include <mutex>
#include <thread>

#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"

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
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-wp_"
                                  "basic-wp_init_fin_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google; // NOLINT
};

TEST_F(wp_init_fin_test, init_fin) { // NOLINT
    ASSERT_EQ(Status::OK, wp::fin());
    ASSERT_EQ(Status::WARN_NOT_INIT, wp::fin());
    ASSERT_EQ(Status::OK, wp::init());
    ASSERT_EQ(Status::WARN_ALREADY_INIT, wp::init());
    ASSERT_EQ(Status::OK, wp::fin());
    ASSERT_EQ(Status::OK, wp::init());
}

TEST_F(wp_init_fin_test, after_init) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    wp::wp_meta* wm{};
    ASSERT_EQ(Status::OK, find_wp_meta(st, wm));
    auto wps{wm->get_wped()};
    ASSERT_EQ(0, wp::wp_meta::find_min_ep(wps));
    ASSERT_EQ(0, wp::wp_meta::find_min_id(wps));
}

} // namespace shirakami::testing
