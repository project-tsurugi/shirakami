
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "shirakami/interface.h"
#include "test_tool.h"


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class Visio_TestCase
    : public ::testing::TestWithParam<
              std::tuple<transaction_type, transaction_type, transaction_type,
                         transaction_type, bool, bool, bool, bool>> {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "anomaly-Visio-TestCase-Visio-TestCase_1a_test");
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

INSTANTIATE_TEST_SUITE_P( // NOLINT
        TxTypePair, Visio_TestCase,
        ::testing::Values(
                std::make_tuple(transaction_type::SHORT,
                                transaction_type::SHORT,
                                transaction_type::SHORT,
                                transaction_type::SHORT, true, false, true,
                                false), // c1
                std::make_tuple(transaction_type::SHORT,
                                transaction_type::SHORT,
                                transaction_type::SHORT, transaction_type::LONG,
                                true, false, true, true), // c2
                std::make_tuple(transaction_type::SHORT,
                                transaction_type::SHORT, transaction_type::LONG,
                                transaction_type::SHORT, true, false, true,
                                false), // c3
                std::make_tuple(transaction_type::SHORT, transaction_type::LONG,
                                transaction_type::SHORT,
                                transaction_type::SHORT, true, true, false,
                                true) // c4
                ));

TEST_P(Visio_TestCase, test_1) { // NOLINT
    // Write Crown 1 WP が消えていく場合
    transaction_type t1_type = std::get<0>(GetParam());
    transaction_type t2_type = std::get<1>(GetParam());
    transaction_type t3_type = std::get<2>(GetParam());
    transaction_type t4_type = std::get<3>(GetParam());
    bool t1_can_commit = std::get<4>(GetParam());
    bool t2_can_commit = std::get<5>(GetParam());
    bool t3_can_commit = std::get<6>(GetParam());
    bool t4_can_commit = std::get<7>(GetParam());
    bool t2_was_finished = false;
    bool t3_was_finished = false;
    bool t4_was_finished = false;

    // setup
    Storage stx{};
    Storage sty{};
    Storage stz{};
    Storage sta{};
    ASSERT_OK(create_storage("x", stx));
    ASSERT_OK(create_storage("y", sty));
    ASSERT_OK(create_storage("z", stz));
    ASSERT_OK(create_storage("a", sta));

    // prepare
    Token t1;
    Token t2;
    Token t3;
    Token t4;
    ASSERT_OK(enter(t1));
    ASSERT_OK(enter(t2));
    ASSERT_OK(enter(t3));
    ASSERT_OK(enter(t4));

    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));
    ASSERT_OK(upsert(t1, stx, "x", "0"));
    ASSERT_OK(upsert(t1, sty, "y", "0"));
    ASSERT_OK(upsert(t1, stz, "z", "0"));
    ASSERT_OK(upsert(t1, sta, "a", "0"));
    ASSERT_OK(commit(t1));

    // test
    // t1 read x
    if (t1_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t1, t1_type}));
    } else if (t1_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t1, t1_type, {sty}}));
        wait_epoch_update();
    } else {
        LOG(FATAL);
    }
    std::string buf{};
    ASSERT_OK(search_key(t1, stx, "x", buf));
    ASSERT_EQ(buf, "0");

    // t2 read y
    if (t2_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t2, t2_type}));
    } else if (t2_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t2, t2_type, {stz}}));
        wait_epoch_update();
    } else {
        LOG(FATAL);
    }
    if (t2_type == transaction_type::SHORT &&
        t1_type == transaction_type::LONG) {
        // fail case
        ASSERT_EQ(Status::ERR_CC, search_key(t2, sty, "y", buf));
        t2_was_finished = true;
    } else {
        // success case
        ASSERT_OK(search_key(t2, sty, "y", buf));
        ASSERT_EQ(buf, "0");
    }

    // t1 write y and commit
    ASSERT_OK(upsert(t1, sty, "y", "1"));
    if (t1_can_commit) {
        ASSERT_OK(commit(t1));
    } else {
        ASSERT_EQ(Status::ERR_CC, commit(t1));
    }

    // t3 read z
    if (t3_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t3, t3_type}));
    } else if (t3_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t3, t3_type, {sta}}));
        wait_epoch_update();
    } else {
        LOG(FATAL);
    }
    if (t3_type == transaction_type::SHORT &&
        t2_type == transaction_type::LONG) {
        // fail case
        ASSERT_EQ(Status::ERR_CC, search_key(t3, stz, "z", buf));
        t3_was_finished = true;
    } else {
        // success case
        ASSERT_OK(search_key(t3, stz, "z", buf));
        ASSERT_EQ(buf, "0");
    }

    // t2 write z and commit
    if (!t2_was_finished) {
        ASSERT_OK(upsert(t2, stz, "z", "2"));
        if (t2_can_commit) {
            ASSERT_OK(commit(t2));
        } else {
            ASSERT_EQ(Status::ERR_CC, commit(t2));
        }
    }

    // t4 read a
    if (t4_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t4, t4_type}));
    } else if (t4_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t4, t4_type, {stx}}));
        wait_epoch_update();
    } else {
        LOG(FATAL);
    }
    if (t4_type == transaction_type::SHORT &&
        t3_type == transaction_type::LONG) {
        // fail case
        ASSERT_EQ(Status::ERR_CC, search_key(t4, sta, "a", buf));
        t4_was_finished = true;
    } else {
        // success case
        ASSERT_OK(search_key(t4, sta, "a", buf));
        ASSERT_EQ(buf, "0");
    }

    // t3 write z and commit
    if (!t3_was_finished) {
        ASSERT_OK(upsert(t3, sta, "a", "3"));
        if (t3_can_commit) {
            ASSERT_OK(commit(t3));
        } else {
            ASSERT_EQ(Status::ERR_CC, commit(t3));
        }
    }

    // t4 write z and commit
    if (!t4_was_finished) {
        ASSERT_OK(upsert(t4, stx, "x", "4"));
        if (t4_can_commit) {
            ASSERT_OK(commit(t4));
        } else {
            ASSERT_EQ(Status::ERR_CC, commit(t4));
        }
    }

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
    ASSERT_OK(leave(t3));
    ASSERT_OK(leave(t4));
}

} // namespace shirakami::testing
