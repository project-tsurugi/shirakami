
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "index/yakushima/include/interface.h"

#include "test_tool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

namespace shirakami::testing {

class tsurugi_issue672_test : public ::testing::TestWithParam<bool> {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue672_test");
        FLAGS_stderrthreshold = 0;
        init_for_test();
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

INSTANTIATE_TEST_SUITE_P(insert_interior_from_border, tsurugi_issue672_test,
                         ::testing::Values(true, false));

TEST_P(tsurugi_issue672_test,         // NOLINT
       insert_interior_from_border) { // NOLINT
    // prepare
    std::size_t entry_size{0};
    if (GetParam()) {
        // insert interior from border
        entry_size = 16;
    } else {
        // insert interior from interor
        entry_size = 15 * 16 + 1;
    }

    Storage st{};
    ASSERT_OK(create_storage("tab", st));

    Token t1{};
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin({t1, transaction_options::transaction_type::SHORT}));
    ScanHandle shd{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(t1, st, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, shd));

    std::string common_prefix(8, '0');
    for (std::size_t i = 0; i < entry_size; ++i) {
        std::string key = common_prefix + std::to_string(i);
        ASSERT_OK(insert(t1, st, key, "0"));
    }

    ASSERT_OK(commit(t1));

    // cleanup
    ASSERT_OK(leave(t1));
}

} // namespace shirakami::testing