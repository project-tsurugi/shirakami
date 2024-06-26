
#include <mutex>
#include <memory>

#include "shirakami/interface.h"
#include "gtest/gtest.h"
#include "glog/logging.h"
#include "shirakami/api_diagnostic.h"
#include "shirakami/scheme.h"

namespace shirakami::testing {

using namespace shirakami;

class c_helper : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-common-"
                                  "helper-c_helper_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(c_helper, leave) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::WARN_NOT_IN_A_SESSION, leave(s));
    ASSERT_EQ(Status::WARN_INVALID_ARGS, leave(nullptr));
}

} // namespace shirakami::testing
