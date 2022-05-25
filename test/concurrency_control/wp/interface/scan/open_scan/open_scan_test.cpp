
#include <mutex>


#include "concurrency_control/wp/include/garbage.h"
#include "concurrency_control/wp/include/session.h"

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class open_scan_test : public ::testing::Test { // NOLINT

public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-common-"
                                  "scan-c_open_scan_test");
        FLAGS_stderrthreshold = 0;                      // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

void wait_change_epoch() {
    auto ce{epoch::get_global_epoch()};
    for (;;) {
        if (ce != epoch::get_global_epoch()) { break; }
        _mm_pause();
    }
}

TEST_F(open_scan_test,            // NOLINT
       avoid_premature_by_wait) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token s{};
    ASSERT_EQ(enter(s), Status::OK);
    ASSERT_EQ(tx_begin(s, false, true, {st}), Status::OK);
    wait_change_epoch();
    ScanHandle hd{};
    ASSERT_NE(open_scan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        hd),
              Status::WARN_PREMATURE);
    ASSERT_EQ(leave(s), Status::OK);
}

} // namespace shirakami::testing
