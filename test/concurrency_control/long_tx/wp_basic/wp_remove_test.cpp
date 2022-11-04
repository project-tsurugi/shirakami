
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

class wp_remove_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-wp_"
                                  "basic-wp_remove_test");
        FLAGS_stderrthreshold = 0;        // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google; // NOLINT
};

TEST_F(wp_remove_test, single_remove) { // NOLINT
    wp::wp_meta meta{};
    meta.set_wped(0, {1, 1});
    meta.set_wped_used(0, true);
    auto rv = meta.get_wped();
    ASSERT_EQ(rv.at(0).first, 1);
    ASSERT_EQ(rv.at(0).second, 1);
    auto& wu{meta.get_wped_used()};
    ASSERT_EQ(wu.test(0), true);
    ASSERT_EQ(wu.count(), 1);
    ASSERT_EQ(Status::OK, meta.remove_wp(1));
    rv = meta.get_wped();
    ASSERT_EQ(rv.at(0).first, 0);
    ASSERT_EQ(rv.at(0).second, 0);
    ASSERT_EQ(wu.test(0), false);
    ASSERT_EQ(wu.count(), 0);
}

TEST_F(wp_remove_test, multi_remove) { // NOLINT
    wp::wp_meta meta{};
    // register 1, 2 by dev func
    meta.set_wped(0, {1, 1});
    meta.set_wped(1, {2, 2});
    meta.set_wped_used(0, true);
    meta.set_wped_used(1, true);

    // check prepare
    auto rv = meta.get_wped();
    ASSERT_EQ(rv.at(0).first, 1);
    ASSERT_EQ(rv.at(0).second, 1);
    ASSERT_EQ(rv.at(1).first, 2);
    ASSERT_EQ(rv.at(1).second, 2);
    auto& wu{meta.get_wped_used()};
    ASSERT_EQ(wu.test(0), true);
    ASSERT_EQ(wu.test(1), true);
    ASSERT_EQ(wu.test(2), false);
    ASSERT_EQ(wu.count(), 2);

    // try remove 2
    ASSERT_EQ(Status::OK, meta.remove_wp(2));

    // check result
    rv = meta.get_wped();
    ASSERT_EQ(rv.at(0).first, 1);
    ASSERT_EQ(rv.at(0).second, 1);
    ASSERT_EQ(rv.at(2).first, 0);
    ASSERT_EQ(rv.at(2).second, 0);
    ASSERT_EQ(wu.test(0), true);
    ASSERT_EQ(wu.test(1), false);
    ASSERT_EQ(wu.test(2), false);
    ASSERT_EQ(wu.count(), 1);

    // try remove 1
    ASSERT_EQ(Status::OK, meta.remove_wp(1));

    // check result
    rv = meta.get_wped();
    ASSERT_EQ(rv.at(0).first, 0);
    ASSERT_EQ(rv.at(0).second, 0);
    ASSERT_EQ(wu.test(0), false);
    ASSERT_EQ(wu.count(), 0);
}

} // namespace shirakami::testing
