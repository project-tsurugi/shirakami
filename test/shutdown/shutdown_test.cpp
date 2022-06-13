
#include <array>
#include <mutex>

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class shutdown_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-shut_down-shutdown_test_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google;
};

TEST_F(shutdown_test, fin) { // NOLINT
    ASSERT_EQ(init(), Status::OK);
    // meaningless fin
    fin();
    fin();
}

} // namespace shirakami::testing
