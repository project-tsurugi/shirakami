
#include <mutex>
#include <vector>

#include "clock.h"

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;



namespace shirakami::testing {

class tsurugi_issue232 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue232");
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

// for issue#232

TEST_F(tsurugi_issue232, 20230609_by_hishidama) { // NOLINT
                                                  // prepare
    Storage st1{};
    Storage st2{};
    ASSERT_OK(create_storage("test1", st1));
    ASSERT_OK(create_storage("test2", st2));
    Token s1{};
    Token s2{};
    ASSERT_OK(enter(s1));
    ASSERT_OK(enter(s2));
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(insert(s1, st1, "", "v1"));
    ASSERT_OK(insert(s1, st2, "", "v1"));
    ASSERT_OK(commit(s1));

    ASSERT_EQ(Status::OK, tx_begin({s1,
                                    transaction_options::transaction_type::LONG,
                                    {st1},
                                    {{st1}, {}}}));
    ASSERT_EQ(Status::OK, tx_begin({s2,
                                    transaction_options::transaction_type::LONG,
                                    {st2},
                                    {{st2}, {}}}));
    wait_epoch_update();
    ASSERT_OK(update(s1, st1, "", "v2"));
    ASSERT_OK(update(s2, st2, "", "v2"));
    // test
    ASSERT_OK(commit(s2));

    // cleanup
    ASSERT_OK(leave(s1));
    ASSERT_OK(leave(s2));
}

} // namespace shirakami::testing