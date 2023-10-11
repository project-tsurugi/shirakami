
#include <atomic>
#include <functional>

#include "shirakami/interface.h"
#include "test_tool.h"

#include "glog/logging.h"
#include "gtest/gtest.h"


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class tsurugi_issue390_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue390_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(tsurugi_issue390_test, check_commit_callback) { // NOLINT
    std::atomic<durability_marker_type> current_durability_marker{};
    register_durability_callback([&](durability_marker_type dm) {
      current_durability_marker = dm;
    });

    Storage st{};
    ASSERT_OK(create_storage("test", st));

    Token s{};
    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();

    Token s2{};
    ASSERT_OK(enter(s2));
    ASSERT_OK(tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s2, st, "", ""));
    durability_marker_type s2_durability_marker{};
    std::atomic_bool s2_called{false};
    ASSERT_TRUE(commit(s2, [&](Status rs, reason_code ,
                                  durability_marker_type dm) {
        ASSERT_EQ(rs, Status::OK);
        s2_durability_marker = dm;
        s2_called = true;
    }));
    while (! s2_called) { _mm_pause(); }
    while (current_durability_marker < s2_durability_marker) { _mm_pause(); }

    std::atomic_bool s_called{false};
    durability_marker_type s_durability_marker{};
    ASSERT_TRUE(commit(s, [&](Status rs, reason_code , durability_marker_type dm) {
        ASSERT_EQ(rs, Status::OK);
        s_called = true;
        s_durability_marker = dm;
    }));
    while (! s_called) { _mm_pause(); }
    while (current_durability_marker < s_durability_marker) { _mm_pause(); }

    // cleanup
    ASSERT_OK(leave(s));
    ASSERT_OK(leave(s2));
}

} // namespace shirakami::testing
