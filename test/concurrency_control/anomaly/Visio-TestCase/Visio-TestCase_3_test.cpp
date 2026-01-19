
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "test_tool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

namespace shirakami::testing {

class Visio_TestCase : public ::testing::TestWithParam<
                               std::tuple<transaction_type, transaction_type,
                                          transaction_type, bool, bool, bool>> {
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-"
                "anomaly-Visio-TestCase-Visio-TestCase_3_test");
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
        tx_type_and_result_pair, Visio_TestCase,
        ::testing::Values( // param and result list
                std::make_tuple(transaction_type::SHORT,
                                transaction_type::SHORT,
                                transaction_type::SHORT, true, true,
                                true), // c1
                std::make_tuple(transaction_type::SHORT,
                                transaction_type::SHORT, transaction_type::LONG,
                                true, false,
                                true), // c2
                std::make_tuple(transaction_type::SHORT, transaction_type::LONG,
                                transaction_type::SHORT, true, true,
                                true), // c3
                std::make_tuple(transaction_type::LONG, transaction_type::SHORT,
                                transaction_type::SHORT, true, true,
                                false), // c4
                std::make_tuple(transaction_type::SHORT, transaction_type::LONG,
                                transaction_type::LONG, true, true,
                                true), // c5
                std::make_tuple(transaction_type::LONG, transaction_type::SHORT,
                                transaction_type::LONG, true, false,
                                true), // c6
                std::make_tuple(transaction_type::LONG, transaction_type::LONG,
                                transaction_type::SHORT, true, true,
                                false), // c7
                std::make_tuple(transaction_type::LONG, transaction_type::LONG,
                                transaction_type::LONG, true, true,
                                false) // c8
                ));

TEST_P(Visio_TestCase, test_1) { // NOLINT
    // Shirakami False Positive: serializableではあるが、判定はnot serializable
    transaction_type t1_type = std::get<0>(GetParam());
    transaction_type t2_type = std::get<1>(GetParam());
    transaction_type t3_type = std::get<2>(GetParam());
    if (t1_type == transaction_type::LONG || t2_type == transaction_type::LONG || t3_type == transaction_type::LONG) { GTEST_SKIP() << "LONG is not supported"; }
    bool t1_can_commit = std::get<3>(GetParam());
    bool t2_can_commit = std::get<4>(GetParam());
    bool t3_can_commit = std::get<5>(GetParam());
    bool t3_was_finished = false;

    std::atomic<Status> cb_rc1{};
    std::atomic<Status> cb_rc2{};
    std::atomic<Status> cb_rc3{};
    [[maybe_unused]] reason_code rc1{};
    [[maybe_unused]] reason_code rc2{};
    [[maybe_unused]] reason_code rc3{};
    std::atomic<bool> was_called2{false};
    std::atomic<bool> was_called3{false};
    auto cb1 = [&cb_rc1, &rc1](Status rs, [[maybe_unused]] reason_code rc,
                               [[maybe_unused]] durability_marker_type dm) {
        cb_rc1.store(rs, std::memory_order_release);
        rc1 = rc;
    };
    auto cb2 = [&cb_rc2,
                &was_called2](Status rs, [[maybe_unused]] reason_code rc,
                              [[maybe_unused]] durability_marker_type dm) {
        cb_rc2.store(rs, std::memory_order_release);
        was_called2.store(true, std::memory_order_release);
    };
    auto cb3 = [&cb_rc3,
                &was_called3](Status rs, [[maybe_unused]] reason_code rc,
                              [[maybe_unused]] durability_marker_type dm) {
        cb_rc3.store(rs, std::memory_order_release);
        was_called3.store(true, std::memory_order_release);
    };

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
    ASSERT_OK(enter(t1));
    ASSERT_OK(enter(t2));
    ASSERT_OK(enter(t3));

    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));
    ASSERT_OK(upsert(t1, stx, "x", "0"));
    ASSERT_OK(upsert(t1, sty, "y", "0"));
    ASSERT_OK(upsert(t1, stz, "z", "0"));
    ASSERT_OK(commit(t1));

    // test
    // t1 read x
    if (t1_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t1, t1_type}));
    } else if (t1_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t1, t1_type, {stx, sty}}));
        wait_epoch_update();
    } else {
        LOG(FATAL);
    }
    std::string buf{};
    ASSERT_OK(search_key(t1, stx, "x", buf));
    ASSERT_EQ(buf, "0");

    // t2 read z
    if (t2_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t2, t2_type}));
    } else if (t2_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t2, t2_type, {sta}}));
        wait_epoch_update();
    } else {
        LOG(FATAL);
    }
    ASSERT_OK(search_key(t2, stz, "z", buf));
    ASSERT_EQ(buf, "0");

    // t1 write y
    ASSERT_OK(upsert(t1, sty, "y", "1"));

    // t3 read y
    if (t3_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t3, t3_type}));
    } else if (t3_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t3, t3_type, {stz}}));
        wait_epoch_update();
    } else {
        LOG(FATAL);
    }
    if (t3_type == transaction_type::SHORT &&
        t1_type == transaction_type::LONG) {
        // fail case
        ASSERT_EQ(Status::ERR_CC, search_key(t3, sty, "y", buf));
        t3_was_finished = true;
    } else {
        // success case
        ASSERT_OK(search_key(t3, sty, "y", buf));
        ASSERT_EQ(buf, "0");
    }

    // t2 write a and commit
    ASSERT_OK(upsert(t2, sta, "a", "2"));
    commit(t2, cb2);

    // t3 write z and commit
    if (!t3_was_finished) {
        ASSERT_OK(upsert(t3, stz, "z", "3"));
        commit(t3, cb3);
    }


    // t1 write x and commit
    ASSERT_OK(upsert(t1, stx, "x", "1"));
    commit(t1, cb1);

    // verify t1
    // wait commit
    if (t1_can_commit) {
        ASSERT_EQ(cb_rc1, Status::OK);
    } else {
        ASSERT_EQ(cb_rc1, Status::ERR_CC);
    }

    // verify t2
    // wait commit
    while (!was_called2) { _mm_pause(); }
    if (t2_can_commit) {
        ASSERT_EQ(cb_rc2, Status::OK);
    } else {
        ASSERT_EQ(cb_rc2, Status::ERR_CC);
    }

    // verify t3
    if (!t3_was_finished) {
        // wait commit
        while (!was_called3) { _mm_pause(); }
        if (t3_can_commit) {
            ASSERT_EQ(cb_rc3, Status::OK);
        } else {
            ASSERT_EQ(cb_rc3, Status::ERR_CC);
        }
    }

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
    ASSERT_OK(leave(t3));
}

} // namespace shirakami::testing
