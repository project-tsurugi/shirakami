#include "kvs/interface.h"

#include <array>
#include <bitset>

#include "gtest/gtest.h"

// shirakami-impl interface library
#include "concurrency_control/silo_variant/include/scheme.h"
#include "compiler.h"
#include "tuple_local.h"

namespace shirakami::testing {

using namespace shirakami;

class tid_test : public ::testing::Test {  // NOLINT
public:
    void SetUp() override { init(); }  // NOLINT

    void TearDown() override { fin(); }
};

TEST_F(tid_test, tidword) {  // NOLINT
    // check the alignment of union
    tid_word tidword;
    tidword.set_epoch(1);
    tidword.set_lock(true);
    [[maybe_unused]] uint64_t res = tidword.get_obj();
    // std::cout << std::bitset<64>(res) << std::endl;
}

}  // namespace shirakami::testing
