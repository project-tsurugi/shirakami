
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "test_tool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;



namespace shirakami::testing {

class Visio_TestCase
    : public ::testing::TestWithParam<
              std::tuple<transaction_type, transaction_type, transaction_type,
                         transaction_type, transaction_type, transaction_type,
                         bool, bool, bool, bool, bool, bool>> {
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-"
                "anomaly-Visio-TestCase-Visio-Test'Case_11h_1_test");
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
                std::make_tuple(transaction_type::LONG, transaction_type::SHORT,
                                transaction_type::SHORT, transaction_type::LONG,
                                transaction_type::SHORT, transaction_type::LONG,
                                true, false, true, true, false,
                                true), // c29
                std::make_tuple(transaction_type::SHORT, transaction_type::LONG,
                                transaction_type::LONG, transaction_type::SHORT,
                                transaction_type::SHORT, transaction_type::LONG,
                                false, true, true, true, true,
                                false) // c30
                ));

TEST_P(Visio_TestCase, test) { // NOLINT
    /**
     * read only が crown を形成するケース(4項) ROx2+RoWx2
    */
    transaction_type t1_type = std::get<0>(GetParam());
    transaction_type t2_type = std::get<1>(GetParam());
    transaction_type t3_type = std::get<2>(GetParam());
    transaction_type t4_type = std::get<3>(GetParam());
    transaction_type t5_type = std::get<4>(GetParam());
    transaction_type t6_type = std::get<5>(GetParam());
    [[maybe_unused]] bool t1_can_commit = std::get<6>(GetParam());
    [[maybe_unused]] bool t2_can_commit = std::get<7>(GetParam());
    [[maybe_unused]] bool t3_can_commit = std::get<8>(GetParam());
    [[maybe_unused]] bool t4_can_commit = std::get<9>(GetParam());
    [[maybe_unused]] bool t5_can_commit = std::get<10>(GetParam());
    [[maybe_unused]] bool t6_can_commit = std::get<11>(GetParam());
    [[maybe_unused]] bool t1_was_finished{false};
    [[maybe_unused]] bool t2_was_finished{false};
    [[maybe_unused]] bool t3_was_finished{false};
    [[maybe_unused]] bool t4_was_finished{false};
    [[maybe_unused]] bool t5_was_finished{false};
    [[maybe_unused]] bool t6_was_finished{false};

    [[maybe_unused]] std::atomic<Status> cb_rc1{};
    [[maybe_unused]] std::atomic<Status> cb_rc2{};
    [[maybe_unused]] std::atomic<Status> cb_rc3{};
    [[maybe_unused]] std::atomic<Status> cb_rc4{};
    [[maybe_unused]] std::atomic<Status> cb_rc5{};
    [[maybe_unused]] std::atomic<Status> cb_rc6{};
    [[maybe_unused]] std::atomic<bool> was_called_1{false};
    [[maybe_unused]] std::atomic<bool> was_called_2{false};
    [[maybe_unused]] std::atomic<bool> was_called_3{false};
    [[maybe_unused]] std::atomic<bool> was_called_4{false};
    [[maybe_unused]] std::atomic<bool> was_called_5{false};
    [[maybe_unused]] std::atomic<bool> was_called_6{false};
    [[maybe_unused]] reason_code rc1{};
    [[maybe_unused]] reason_code rc2{};
    [[maybe_unused]] reason_code rc3{};
    [[maybe_unused]] reason_code rc4{};
    [[maybe_unused]] reason_code rc5{};
    [[maybe_unused]] reason_code rc6{};
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
    auto cb4 = [&cb_rc4, &rc4,
                &was_called_4](Status rs, reason_code rc,
                               [[maybe_unused]] durability_marker_type dm) {
        cb_rc4.store(rs, std::memory_order_release);
        rc4 = rc;
        was_called_4 = true;
    };
    auto cb5 = [&cb_rc5, &rc5,
                &was_called_5](Status rs, reason_code rc,
                               [[maybe_unused]] durability_marker_type dm) {
        cb_rc5.store(rs, std::memory_order_release);
        rc5 = rc;
        was_called_5 = true;
    };
    auto cb6 = [&cb_rc6, &rc6,
                &was_called_6](Status rs, reason_code rc,
                               [[maybe_unused]] durability_marker_type dm) {
        cb_rc6.store(rs, std::memory_order_release);
        rc6 = rc;
        was_called_6 = true;
    };

    // setup
    Storage sta{};
    Storage stx{};
    Storage stu{};
    Storage stv{};
    ASSERT_OK(create_storage("a", sta));
    ASSERT_OK(create_storage("x", stx));
    ASSERT_OK(create_storage("u", stu));
    ASSERT_OK(create_storage("v", stv));

    // prepare
    Token t1;
    Token t2;
    Token t3;
    Token t4;
    Token t5;
    Token t6;
    ASSERT_OK(enter(t1));
    ASSERT_OK(enter(t2));
    ASSERT_OK(enter(t3));
    ASSERT_OK(enter(t4));
    ASSERT_OK(enter(t5));
    ASSERT_OK(enter(t6));

    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));
    ASSERT_OK(upsert(t1, sta, "a", "0"));
    ASSERT_OK(upsert(t1, stx, "x", "0"));
    ASSERT_OK(upsert(t1, stu, "u", "0"));
    ASSERT_OK(upsert(t1, stv, "v", "0"));
    ASSERT_OK(commit(t1));

    // test

    stop_epoch();

    // t1, 2, 3, 4 begin
    if (t1_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t1, t1_type}));
    } else if (t1_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t1, t1_type, {stx}}));
    } else {
        LOG(FATAL);
    }
    if (t2_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t2, t2_type}));
    } else if (t2_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t2, t2_type, {stv}}));
    } else {
        LOG(FATAL);
    }
    if (t3_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t3, t3_type}));
    } else if (t3_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t3, t3_type, {stu}}));
    } else {
        LOG(FATAL);
    }
    if (t4_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t4, t4_type}));
    } else if (t4_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t4, t4_type, {sta}}));
    } else {
        LOG(FATAL);
    }

    resume_epoch();
    wait_epoch_update(); // for some ltx

    // t1 read a
    std::string buf{};
    if (t1_type == transaction_type::SHORT &&
        t4_type == transaction_type::LONG) {
        // fail case
        ASSERT_EQ(Status::ERR_CC, search_key(t1, sta, "a", buf));
        t1_was_finished = true;
    } else {
        // success case
        ASSERT_OK(search_key(t1, sta, "a", buf));
        ASSERT_EQ(buf, "0");
    }

    // t2 read u
    if (t2_type == transaction_type::SHORT &&
        t3_type == transaction_type::LONG) {
        // fail case
        ASSERT_EQ(Status::ERR_CC, search_key(t2, stu, "u", buf));
        t2_was_finished = true;
    } else {
        // success case
        ASSERT_OK(search_key(t2, stu, "u", buf));
        ASSERT_EQ(buf, "0");
    }

    // t3 write u, commit
    ASSERT_OK(upsert(t3, stu, "u", "3"));
    commit(t3, cb3);

    // t4 write a, commit
    ASSERT_OK(upsert(t4, sta, "a", "4"));
    commit(t4, cb4);

    // t6, t5 begin
    stop_epoch();
    if (t6_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t6, t6_type}));
    } else if (t6_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t6, t6_type}));
    } else {
        LOG(FATAL);
    }
    if (t5_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t5, t5_type}));
    } else if (t5_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t5, t5_type}));
    } else {
        LOG(FATAL);
    }
    resume_epoch();
    wait_epoch_update();

    // t6 read a
    ASSERT_OK(search_key(t6, sta, "a", buf));
    ASSERT_EQ(buf, "4");

    // t5 read u
    ASSERT_OK(search_key(t5, stu, "u", buf));
    ASSERT_EQ(buf, "3");

    // t6 read v, commit
    if (t6_type == transaction_type::SHORT &&
        t2_type == transaction_type::LONG) {
        // fail case
        ASSERT_EQ(Status::ERR_CC, search_key(t6, stv, "v", buf));
        t6_was_finished = true;
    } else {
        // success case
        ASSERT_OK(search_key(t6, stv, "v", buf));
        ASSERT_EQ(buf, "0");
        commit(t6, cb6);
    }

    // t5 read x, commit
    if (t5_type == transaction_type::SHORT &&
        t1_type == transaction_type::LONG) {
        // fail case
        ASSERT_EQ(Status::ERR_CC, search_key(t5, stx, "x", buf));
        t5_was_finished = true;
    } else {
        // succes case
        ASSERT_OK(search_key(t5, stx, "x", buf));
        ASSERT_EQ(buf, "0");
        commit(t5, cb5);
    }

    // t2 write v, commit
    if (!t2_was_finished) {
        ASSERT_OK(upsert(t2, stv, "v", "2"));
        commit(t2, cb2);
    }

    // t1 write x, commit
    if (!t1_was_finished) {
        ASSERT_OK(upsert(t1, stx, "x", "1"));
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

    if (!t2_was_finished) {
        if (t2_type == transaction_type::LONG) {
            while (!was_called_2) { _mm_pause(); }
        }
        if (t2_can_commit) {
            ASSERT_EQ(cb_rc2, Status::OK);
        } else {
            ASSERT_EQ(cb_rc2, Status::ERR_CC);
        }
    }

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

    if (!t4_was_finished) {
        if (t4_type == transaction_type::LONG) {
            while (!was_called_4) { _mm_pause(); }
        }
        if (t4_can_commit) {
            ASSERT_EQ(cb_rc4, Status::OK);
        } else {
            ASSERT_EQ(cb_rc4, Status::ERR_CC);
        }
    }

    if (!t5_was_finished) {
        if (t5_type == transaction_type::LONG) {
            while (!was_called_5) { _mm_pause(); }
        }
        if (t5_can_commit) {
            ASSERT_EQ(cb_rc5, Status::OK);
        } else {
            ASSERT_EQ(cb_rc5, Status::ERR_CC);
        }
    }

    if (!t6_was_finished) {
        if (t6_type == transaction_type::LONG) {
            while (!was_called_6) { _mm_pause(); }
        }
        if (t6_can_commit) {
            ASSERT_EQ(cb_rc6, Status::OK);
        } else {
            ASSERT_EQ(cb_rc6, Status::ERR_CC);
        }
    }

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
    ASSERT_OK(leave(t3));
    ASSERT_OK(leave(t4));
    ASSERT_OK(leave(t5));
    ASSERT_OK(leave(t6));
}

} // namespace shirakami::testing
