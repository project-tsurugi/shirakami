
#include <array>
#include <mutex>

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class start_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-start-start_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google;
};

TEST_F(start_test, double_start) { // NOLINT
    ASSERT_EQ(init(), Status::OK);
    ASSERT_EQ(init(), Status::WARN_ALREADY_INIT);
    fin();
}

TEST_F(start_test, valid_recovery_invalid_log_directory) { // NOLINT
    ASSERT_EQ(init(true, ""), Status::OK);
    fin();
}

} // namespace shirakami::testing
