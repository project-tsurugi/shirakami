
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/record.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/version.h"

#include "shirakami/api_result.h"
#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class result_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-function-"
                                  "result-result_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google_, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(result_test, reason_code) { // NOLINT
    // check operator<<
    LOG(INFO) << reason_code::CC_LTX_READ_UPPER_BOUND_VIOLATION;
}

TEST_F(result_test, result_info) { // NOLINT
    // test constructor
    result_info ri{reason_code::CC_LTX_READ_UPPER_BOUND_VIOLATION};
    ASSERT_EQ(ri.get_reason_code(),
              reason_code::CC_LTX_READ_UPPER_BOUND_VIOLATION);

    // test: set_reason_code
    ri.set_reason_code(reason_code::CC_LTX_READ_UPPER_BOUND_VIOLATION);
    ASSERT_EQ(ri.get_reason_code(),
              reason_code::CC_LTX_READ_UPPER_BOUND_VIOLATION);
}

} // namespace shirakami::testing