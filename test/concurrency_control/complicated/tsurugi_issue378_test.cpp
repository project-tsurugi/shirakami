
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

class tsurugi_issue378 : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue378");
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

TEST_F(tsurugi_issue378, simple) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s1, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    ScanHandle hd1{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s1, st, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, hd1));
    ASSERT_EQ(Status::OK, insert(s1, st, "2", "100"));

    ASSERT_EQ(
            Status::OK,
            tx_begin({s2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();

    ScanHandle hd2{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s2, st, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, hd2));
    ASSERT_EQ(Status::OK, insert(s2, st, "3", "100"));

    LOG(INFO);
    ASSERT_EQ(Status::OK, commit(s1));     // NOLINT
    LOG(INFO);
    ASSERT_EQ(Status::ERR_CC, commit(s2)); // NOLINT
    LOG(INFO);

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing