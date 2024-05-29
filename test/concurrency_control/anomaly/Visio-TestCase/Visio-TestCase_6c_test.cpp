
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
                         transaction_type, bool, bool, bool, bool>> {
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-"
                "anomaly-Visio-TestCase-Visio-Test'Case_6c_test");
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
                                transaction_type::LONG, transaction_type::SHORT,
                                true, true, true,
                                false), // c9
                std::make_tuple(transaction_type::LONG, transaction_type::SHORT,
                                transaction_type::LONG, transaction_type::SHORT,
                                true, false, true,
                                true), // c10
                std::make_tuple(transaction_type::LONG, transaction_type::LONG,
                                transaction_type::SHORT,
                                transaction_type::SHORT, true, true, true,
                                false), // c11
                std::make_tuple(transaction_type::SHORT, transaction_type::LONG,
                                transaction_type::LONG, transaction_type::LONG,
                                true, true, true,
                                false) // c12
                ));

TEST_P(Visio_TestCase, test_1) { // NOLINT
    // read crown, read only が crown を形成するケース（三項で順序入れ替え）
    transaction_type t1_type = std::get<0>(GetParam());
    transaction_type t2_type = std::get<1>(GetParam());
    transaction_type t3_type = std::get<2>(GetParam());
    transaction_type t4_type = std::get<3>(GetParam());
    bool t1_can_commit = std::get<4>(GetParam());
    bool t2_can_commit = std::get<5>(GetParam());
    bool t3_can_commit = std::get<6>(GetParam());
    bool t4_can_commit = std::get<7>(GetParam());
    [[maybe_unused]] bool t2_was_finished{false};
    [[maybe_unused]] bool t4_was_finished{false};

    std::atomic<Status> cb_rc1{};
    std::atomic<Status> cb_rc2{};
    std::atomic<Status> cb_rc3{};
    std::atomic<Status> cb_rc4{};
    std::atomic<bool> was_called_1{false};
    std::atomic<bool> was_called_2{false};
    std::atomic<bool> was_called_3{false};
    std::atomic<bool> was_called_4{false};
    [[maybe_unused]] reason_code rc1{};
    [[maybe_unused]] reason_code rc2{};
    [[maybe_unused]] reason_code rc3{};
    [[maybe_unused]] reason_code rc4{};
    auto cb1 = [&cb_rc1, &rc1,
                &was_called_1](Status rs, [[maybe_unused]] reason_code rc,
                               [[maybe_unused]] durability_marker_type dm) {
        cb_rc1.store(rs, std::memory_order_release);
        rc1 = rc;
        was_called_1 = true;
    };
    auto cb2 = [&cb_rc2, &rc2,
                &was_called_2](Status rs, [[maybe_unused]] reason_code rc,
                               [[maybe_unused]] durability_marker_type dm) {
        cb_rc2.store(rs, std::memory_order_release);
        rc2 = rc;
        was_called_2 = true;
    };
    auto cb3 = [&cb_rc3, &rc3,
                &was_called_3](Status rs, [[maybe_unused]] reason_code rc,
                               [[maybe_unused]] durability_marker_type dm) {
        cb_rc3.store(rs, std::memory_order_release);
        rc3 = rc;
        was_called_3 = true;
    };
    auto cb4 = [&cb_rc4, &rc4,
                &was_called_4](Status rs, [[maybe_unused]] reason_code rc,
                               [[maybe_unused]] durability_marker_type dm) {
        cb_rc4.store(rs, std::memory_order_release);
        rc4 = rc;
        was_called_4 = true;
    };

    // setup
    Storage sta{};
    Storage stu{};
    Storage stx{};
    ASSERT_OK(create_storage("a", sta));
    ASSERT_OK(create_storage("u", stu));
    ASSERT_OK(create_storage("x", stx));

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
    ASSERT_OK(upsert(t1, sta, "a", "0"));
    ASSERT_OK(upsert(t1, stu, "u", "0"));
    ASSERT_OK(upsert(t1, stx, "x", "0"));
    ASSERT_OK(commit(t1));

    // test
    // t1 begin, read a
    if (t1_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t1, t1_type}));
    } else if (t1_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t1, t1_type, {stx}}));
        wait_epoch_update();
    } else {
        LOG(FATAL);
    }
    std::string buf{};
    ASSERT_OK(search_key(t1, sta, "a", buf));
    ASSERT_EQ(buf, "0");

    // t2 begin, read x
    if (t2_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t2, t2_type}));
    } else if (t2_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t2, t2_type, {stu}}));
        wait_epoch_update();
    } else {
        LOG(FATAL);
    }
    if (t2_type == transaction_type::SHORT &&
        t1_type == transaction_type::LONG) {
        // fail case
        ASSERT_EQ(Status::ERR_CC, search_key(t2, stx, "x", buf));
        t2_was_finished = true;
    } else {
        // success case
        ASSERT_OK(search_key(t2, stx, "x", buf));
        ASSERT_EQ(buf, "0");
    }

    // t1 write x and commit
    ASSERT_OK(upsert(t1, stx, "x", "1"));
    commit(t1, cb1);

    // t3 begin, write a and commit
    if (t3_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t3, t3_type}));
    } else if (t3_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t3, t3_type, {sta}}));
        wait_epoch_update();
    } else {
        LOG(FATAL);
    }
    ASSERT_OK(upsert(t3, sta, "a", "3"));
    commit(t3, cb3);

    // t4 begin, read a
    if (t4_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t4, t4_type}));
    } else if (t4_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t4, t4_type}));
        wait_epoch_update();
    } else {
        LOG(FATAL);
    }
    ASSERT_OK(search_key(t4, sta, "a", buf));
    ASSERT_EQ(buf, "3");

    // t4 read u, commit
    if (t4_type == transaction_type::SHORT &&
        t2_type == transaction_type::LONG) {
        // fail case
        ASSERT_EQ(Status::ERR_CC, search_key(t4, stu, "u", buf));
        t4_was_finished = true;
    } else {
        // success case
        ASSERT_OK(search_key(t4, stu, "u", buf));
        ASSERT_EQ(buf, "0");
        commit(t4, cb4);
    }

    // t2 write z, commit
    if (!t2_was_finished) {
        ASSERT_OK(upsert(t2, stu, "u", "2"));
        commit(t2, cb2);
    }

    // verify t1
    if (t1_can_commit) {
        if (t1_type == transaction_type::LONG) {
            while (!was_called_1) { _mm_pause(); }
        }
        ASSERT_EQ(cb_rc1, Status::OK);
    } else {
        ASSERT_EQ(cb_rc1, Status::ERR_CC);
    }

    // verify t2
    if (!t2_was_finished) {
        if (t2_type == transaction_type::LONG) {
            while (!was_called_2) { _mm_pause(); }
        }
        if (t2_can_commit) {
            ASSERT_EQ(cb_rc2, Status::OK);
        } else {
            ASSERT_EQ(cb_rc2, Status::ERR_CC);
        }
    } else {
        // early abort
        ASSERT_FALSE(t2_can_commit);
    }

    // verify t3
    if (t3_can_commit) {
        if (t3_type == transaction_type::LONG) {
            while (!was_called_3) { _mm_pause(); }
        }
        ASSERT_EQ(cb_rc3, Status::OK);
    } else {
        ASSERT_EQ(cb_rc3, Status::ERR_CC);
    }

    // verify t4
    if (!t4_was_finished) {
        if (t4_type == transaction_type::LONG) {
            while (!was_called_4) { _mm_pause(); }
        }
        if (t4_can_commit) {
            ASSERT_EQ(cb_rc4, Status::OK);
        } else {
            ASSERT_EQ(cb_rc4, Status::ERR_CC);
        }
    } else {
        // early abort
        ASSERT_FALSE(t4_can_commit);
    }

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
    ASSERT_OK(leave(t3));
    ASSERT_OK(leave(t4));
}

} // namespace shirakami::testing
