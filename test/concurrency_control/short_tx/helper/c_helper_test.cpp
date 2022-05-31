
#include <array>
#include <mutex>

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class c_helper : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-common-helper-c_helper_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(false, "/tmp/shirakami_c_helper_test"); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(c_helper, init) { // NOLINT
    ASSERT_EQ(init(), Status::WARN_ALREADY_INIT);
    fin();
    ASSERT_EQ(init(), Status::OK);
}

TEST_F(c_helper, fin) { // NOLINT
    // meaningless fin
    fin();
    fin();
}

} // namespace shirakami::testing
