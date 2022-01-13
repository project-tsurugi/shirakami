
#include <glog/logging.h>

#include "concurrency_control/wp/include/ongoing_tx.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class ongoing_tx_test : public ::testing::Test { // NOLINT
public:
    void SetUp() override { init(); } // NOLINT

    void TearDown() override { fin(); }
};

TEST_F(ongoing_tx_test, simple) { // NOLINT
    ongoing_tx::push(1);
    ASSERT_EQ(ongoing_tx::exist_preceding(2), true);
    ongoing_tx::remove(1);
    ASSERT_EQ(ongoing_tx::exist_preceding(2), false);
}

} // namespace shirakami::testing
