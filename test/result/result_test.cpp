
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/version.h"

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
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google_, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(result_test, reason_code) { // NOLINT
    // check operator<<
    LOG(INFO) << reason_code::COMMITTED_READ_PROTECTION;
    LOG(INFO) << reason_code::READ_UPPER_BOUND;
}

TEST_F(result_test, result_info) { // NOLINT
    // test constructor
    result_info ri{reason_code::COMMITTED_READ_PROTECTION, "test"};
    ASSERT_EQ(ri.get_reason_code(), reason_code::COMMITTED_READ_PROTECTION);
    ASSERT_EQ(ri.get_additional_information(), "test");

    // test: clear_additional_information
    ri.clear_additional_information();
    ASSERT_EQ(ri.get_additional_information(), "");

    // test: set_reason_code
    ri.set_reason_code(reason_code::READ_UPPER_BOUND);
    ASSERT_EQ(ri.get_reason_code(), reason_code::READ_UPPER_BOUND);
}

} // namespace shirakami::testing