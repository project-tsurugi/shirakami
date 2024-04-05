
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
                "anomaly-Visio-TestCase-Visio-Test'Case_8b_test");
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
                std::make_tuple(transaction_type::SHORT, transaction_type::LONG,
                                transaction_type::LONG, false, true,
                                true), // c5
                std::make_tuple(transaction_type::LONG, transaction_type::SHORT,
                                transaction_type::LONG, true, true,
                                true), // c6
                std::make_tuple(transaction_type::LONG, transaction_type::LONG,
                                transaction_type::SHORT, true, true,
                                false), // c7
                std::make_tuple(transaction_type::LONG, transaction_type::LONG,
                                transaction_type::LONG, true, true,
                                true) // c8
                ));

TEST_P(Visio_TestCase, test_1) { // NOLINT
    // Quasi ROA: False positive Batch read は serializable
    transaction_type t1_type = std::get<0>(GetParam());
    transaction_type t2_type = std::get<1>(GetParam());
    transaction_type t3_type = std::get<2>(GetParam());
    bool t1_can_commit = std::get<3>(GetParam());
    bool t2_can_commit = std::get<4>(GetParam());
    bool t3_can_commit = std::get<5>(GetParam());
    bool t1_was_finished{false};
    bool t3_was_finished{false};

    std::atomic<Status> cb_rc1{};
    std::atomic<Status> cb_rc2{};
    std::atomic<Status> cb_rc3{};
    std::atomic<bool> was_called_1{false};
    std::atomic<bool> was_called_2{false};
    std::atomic<bool> was_called_3{false};
    reason_code rc1{};
    reason_code rc2{};
    reason_code rc3{};
    auto cb1 = [&cb_rc1, &rc1,
                &was_called_1](Status rs, reason_code rc,
                               [[maybe_unused]] durability_marker_type dm) {
        cb_rc1.store(rs, std::memory_order_release);
        rc1 = rc;
        was_called_1 = true;
    };
    auto cb2 = [&cb_rc2, &rc2,
                &was_called_2](Status rs, reason_code rc,
                               [[maybe_unused]] durability_marker_type dm) {
        cb_rc2.store(rs, std::memory_order_release);
        rc2 = rc;
        was_called_2 = true;
    };
    auto cb3 = [&cb_rc3, &rc3,
                &was_called_3](Status rs, reason_code rc,
                               [[maybe_unused]] durability_marker_type dm) {
        cb_rc3.store(rs, std::memory_order_release);
        rc3 = rc;
        was_called_3 = true;
    };

    // setup
    Storage stx{};
    Storage sty{};
    ASSERT_OK(create_storage("x", stx));
    ASSERT_OK(create_storage("y", sty));

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
    ASSERT_OK(commit(t1));

    // test

    // stop epoch
    stop_epoch();

    // t1, 2, 3 begin
    if (t1_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t1, t1_type}));
    } else if (t1_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t1, t1_type, {sty}}));
    } else {
        LOG(FATAL);
    }
    if (t2_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t2, t2_type}));
    } else if (t2_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t2, t2_type, {stx}}));
    } else {
        LOG(FATAL);
    }
    if (t3_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t3, t3_type}));
    } else if (t3_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t3, t3_type}));
    } else {
        LOG(FATAL);
    }

    // resume epoch
    resume_epoch();
    wait_epoch_update(); // for some ltx

    // t1 read x
    std::string buf{};
    if (t1_type == transaction_type::SHORT &&
        t2_type == transaction_type::LONG) {
        // fail case
        ASSERT_EQ(Status::ERR_CC, search_key(t1, stx, "x", buf));
        t1_was_finished = true;
    } else {
        // success case
        ASSERT_OK(search_key(t1, stx, "x", buf));
        ASSERT_EQ(buf, "0");
    }

    // t2 read x
    ASSERT_OK(search_key(t2, stx, "x", buf));
    ASSERT_EQ(buf, "0");

    // t1 read y
    if (!t1_was_finished) {
        ASSERT_OK(search_key(t1, sty, "y", buf));
        ASSERT_EQ(buf, "0");
    }

    // t2 write x, commit
    ASSERT_OK(upsert(t2, stx, "x", "2"));
    commit(t2, cb2);

    // t3 read y
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

    // t3 read x, commit
    if (!t3_was_finished) {
        ASSERT_OK(search_key(t3, stx, "x", buf));
        if (t3_type == transaction_type::SHORT) {
            ASSERT_EQ(buf, "2");
        } else {
            ASSERT_EQ(buf, "0");
        }
        commit(t3, cb3);
    }

    // t1 write y, commit
    if (!t1_was_finished) {
        ASSERT_OK(upsert(t1, sty, "y", "1"));
        commit(t1, cb1);
    }

    // verify t1
    if (!t1_was_finished) {
        if (t1_can_commit) {
            ASSERT_EQ(cb_rc1, Status::OK);
        } else {
            ASSERT_EQ(cb_rc1, Status::ERR_CC);
        }
    }

    // verify t2
    if (t2_can_commit) {
        ASSERT_EQ(cb_rc2, Status::OK);
    } else {
        ASSERT_EQ(cb_rc2, Status::ERR_CC);
    }

    // verify t3
    if (!t3_was_finished) {
        if (t3_type == transaction_type::LONG) {
            while (!was_called_3) { _mm_pause(); }
        }
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
