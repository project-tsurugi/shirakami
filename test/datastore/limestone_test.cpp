
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class limestone_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "limestone_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google; // NOLINT
};

void wait_change_epoch() {
    auto ce{epoch::get_global_epoch()};
    for (;;) {
        if (ce != epoch::get_global_epoch()) { break; }
        _mm_pause();
    }
}

TEST_F(limestone_test, check_output) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(Status::OK, register_storage(st));

    Token s{}; // for short
    ASSERT_EQ(Status::OK, enter(s));

    // prepare data
    std::string k{"k"};
    ASSERT_EQ(Status::OK, upsert(s, st, k, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    wait_change_epoch();
    ASSERT_EQ(Status::OK, upsert(s, st, k, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    wait_change_epoch();
    ASSERT_EQ(Status::OK, upsert(s, st, k, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    wait_change_epoch();
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
