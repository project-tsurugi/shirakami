
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "index/yakushima/include/interface.h"

#include "test_tool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

namespace shirakami::testing {

class tsurugi_issue666_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue666_test");
        // FLAGS_stderrthreshold = 0;
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

TEST_F(tsurugi_issue666_test, // NOLINT
       simple) {              // NOLINT
                              // prepare
    Storage yz{};
    ASSERT_OK(create_storage("yz", yz));

    Token t1{};
    Token t2{};
    Token t3{};
    ASSERT_OK(enter(t1));
    ASSERT_OK(enter(t2));
    ASSERT_OK(enter(t3));
    // initialize along param
    ASSERT_OK(
            tx_begin({t1, transaction_options::transaction_type::LONG, {yz}}));
    wait_epoch_update();
    ASSERT_OK(upsert(t1, yz, "1", "0"));
    ASSERT_OK(commit(t1));

    // test senario
    ASSERT_OK(
            tx_begin({t1, transaction_options::transaction_type::LONG, {yz}}));
    wait_epoch_update();
    ASSERT_OK(
            tx_begin({t2, transaction_options::transaction_type::LONG, {yz}}));
    wait_epoch_update();
    ASSERT_OK(
            tx_begin({t3, transaction_options::transaction_type::LONG, {yz}}));
    wait_epoch_update();

    std::string buf{};
    ASSERT_OK(search_key(t1, yz, "1", buf));
    ASSERT_EQ(buf, "0");
    ASSERT_OK(delete_record(t1, yz, "1"));
    ASSERT_OK(upsert(t1, yz, "1", "1"));

    ASSERT_OK(search_key(t2, yz, "1", buf));
    ASSERT_EQ(buf, "0");
    ASSERT_OK(delete_record(t2, yz, "1"));
    ASSERT_OK(upsert(t2, yz, "1", "2"));

    ASSERT_OK(search_key(t3, yz, "1", buf));
    ASSERT_EQ(buf, "0");
    ASSERT_OK(delete_record(t3, yz, "1"));
    ASSERT_OK(upsert(t3, yz, "1", "3"));

    // verify
    ASSERT_OK(commit(t1));
    ASSERT_EQ(Status::ERR_CC, commit(t2));
    ASSERT_EQ(Status::ERR_CC, commit(t3));

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
    ASSERT_OK(leave(t3));
}

} // namespace shirakami::testing
