
#include <mutex>
#include <vector>

#include "clock.h"

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class tsurugi_issue313 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue313");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(tsurugi_issue313, simple) { // NOLINT
    // 28 sec parameter if you execute only this test
    //constexpr std::size_t loop_num = 1000000;
    // 1 sec parameter if you execute only this test
    constexpr std::size_t loop_num = 1;

    Storage st{};
    ASSERT_OK(create_storage("test", st));
    Token s{};
    ASSERT_OK(enter(s));
    SequenceId sid1{};
    SequenceId sid2{};
    ASSERT_OK(create_sequence(&sid1));
    ASSERT_OK(create_sequence(&sid2));
    for (std::size_t i = 1; i <= loop_num; ++i) {
        ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
        SequenceVersion sve{};
        SequenceValue sva{};
        ASSERT_OK(read_sequence(sid1, &sve, &sva));
        LOG(INFO) << sve << ", " << sva;
        ASSERT_OK(update_sequence(s, sid1, i, i));
        ASSERT_OK(read_sequence(sid1, &sve, &sva));
        LOG(INFO) << sve << ", " << sva;
        ASSERT_OK(read_sequence(sid2, &sve, &sva));
        LOG(INFO) << sve << ", " << sva;
        ASSERT_OK(update_sequence(s, sid2, i, i));
        ASSERT_OK(read_sequence(sid2, &sve, &sva));
        LOG(INFO) << sve << ", " << sva;
        ASSERT_OK(commit(s));
    }
    // cleanup
    ASSERT_OK(leave(s));
}

} // namespace shirakami::testing