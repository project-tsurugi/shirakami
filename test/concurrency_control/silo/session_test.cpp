
#include "gtest/gtest.h"

#include "concurrency_control/silo/include/session.h"

#include "concurrency_control/include/tuple_local.h" // sizeof(Tuple)

#include "shirakami/interface.h"

namespace shirakami::testing {

using namespace shirakami;

class session_test : public ::testing::Test {
public:
    void SetUp() override {
        init(); // NOLINT
    }

    void TearDown() override { fin(); }
};

} // namespace shirakami::testing
