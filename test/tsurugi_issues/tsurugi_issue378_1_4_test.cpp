
#include <atomic>
#include <functional>
#include <xmmintrin.h>

#include "shirakami/interface.h"
#include "test_tool.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class tsurugi_issue378_1_4 : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue378_1_4");
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

TEST_F(tsurugi_issue378_1_4, case_5) { // NOLINT
                                       // tx1 point read, tx2 range read
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    // initialized data: [1,100], [2,200], [3,300], [4,400], [5,500], [6,600]
    Token s1{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s1, st, "1", "100"));
    ASSERT_EQ(Status::OK, insert(s1, st, "2", "200"));
    ASSERT_EQ(Status::OK, insert(s1, st, "3", "300"));
    ASSERT_EQ(Status::OK, insert(s1, st, "4", "400"));
    ASSERT_EQ(Status::OK, insert(s1, st, "5", "500"));
    ASSERT_EQ(Status::OK, insert(s1, st, "6", "600"));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    // tx1
    ASSERT_EQ(
            Status::OK,
            tx_begin({s1, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    std::string buf{};
    ASSERT_EQ(Status::OK, search_key(s1, st, "1", buf));
    ASSERT_EQ(Status::OK, insert(s1, st, "7", "700"));
    // tx2
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();

    ScanHandle hd2{};
    ASSERT_EQ(Status::OK, open_scan(s2, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd2));
    for (std::size_t i = 1; i <= 6; ++i) { // NOLINT
        ASSERT_EQ(Status::OK, read_key_from_scan(s2, hd2, buf));
        ASSERT_EQ(buf, std::to_string(i));
        if (i < 6) { // NOLINT
            ASSERT_EQ(Status::OK, next(s2, hd2));
        } else {
            ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s2, hd2));
        }
    }
    ASSERT_EQ(Status::OK, update(s2, st, "1", "150"));

    ASSERT_EQ(Status::OK, commit(s1));     // NOLINT
    ASSERT_EQ(Status::ERR_CC, commit(s2)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(tsurugi_issue378_1_4, case_6) { // NOLINT
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
    std::string buf{};
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              search_key(s1, st, "\x80\x00\x00\x01", buf));
    ASSERT_EQ(Status::OK,
              insert(s1, st, "\x80\x00\x00\x06", "\x7f\xff\xfd\xa7"));

    ASSERT_EQ(
            Status::OK,
            tx_begin({s2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();

    ScanHandle shd{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s2, st, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, shd));
    ASSERT_EQ(Status::OK,
              insert(s2, st, "\x80\x00\x00\x01", "\x7f\xff\xff\x9b"));

    ASSERT_EQ(Status::OK, commit(s1));     // NOLINT
    ASSERT_EQ(Status::ERR_CC, commit(s2)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing