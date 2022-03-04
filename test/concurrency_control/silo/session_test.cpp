
#include "gtest/gtest.h"

#include "concurrency_control/silo/include/session.h"

#include "concurrency_control/include/tuple_local.h" // sizeof(Tuple)

#include "shirakami/interface.h"

namespace shirakami::testing {

using namespace shirakami;

class session_test : public ::testing::Test {
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/session_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

} // namespace shirakami::testing
