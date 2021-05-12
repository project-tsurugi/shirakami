#include "shirakami/interface.h"

#include <array>
#include <bitset>

#include "gtest/gtest.h"

// shirakami-impl interface library
#include "compiler.h"
#include "concurrency_control/include/scheme.h"
#include "tuple_local.h"

namespace shirakami::testing {

using namespace shirakami;

class tid_test : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/test/tid_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(tid_test, tidword) { // NOLINT
    // check the alignment of union
    tid_word tidword;
    tidword.set_epoch(1);
    tidword.set_lock(true);
    [[maybe_unused]] uint64_t res = tidword.get_obj();
    // std::cout << std::bitset<64>(res) << std::endl;
}

} // namespace shirakami::testing
