
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "test_tool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class Visio_TestCase
    : public ::testing::TestWithParam<
              std::tuple<transaction_type, transaction_type, transaction_type,
                         transaction_type, transaction_type, bool, bool, bool,
                         bool, bool>> {
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-"
                "anomaly-Visio-TestCase-Visio-Test'Case_7e_test");
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
                                transaction_type::SHORT, transaction_type::LONG,
                                transaction_type::LONG, transaction_type::LONG,
                                false, true, true, true,
                                true), // c17
                std::make_tuple(transaction_type::SHORT, transaction_type::LONG,
                                transaction_type::SHORT, transaction_type::LONG,
                                transaction_type::LONG, false, true, true, true,
                                true), // c18
                std::make_tuple(transaction_type::LONG, transaction_type::SHORT,
                                transaction_type::SHORT, transaction_type::LONG,
                                transaction_type::LONG, true, true, true, true,
                                false), // c19
                std::make_tuple(transaction_type::SHORT, transaction_type::LONG,
                                transaction_type::LONG, transaction_type::SHORT,
                                transaction_type::LONG, false, true, true, true,
                                true) // c20
                ));

TEST_P(Visio_TestCase, test_1) { // NOLINT
    // read only が crown を形成するケース（3項） ROx2+RoW
    transaction_type t1_type = std::get<0>(GetParam());
    transaction_type t2_type = std::get<1>(GetParam());
    transaction_type t3_type = std::get<2>(GetParam());
    transaction_type t4_type = std::get<3>(GetParam());
    transaction_type t5_type = std::get<4>(GetParam());
    bool t1_can_commit = std::get<5>(GetParam());
    //bool t2_can_commit = std::get<6>(GetParam());
    //bool t3_can_commit = std::get<7>(GetParam());
    //bool t4_can_commit = std::get<8>(GetParam());
    bool t5_can_commit = std::get<9>(GetParam());
    bool t5_was_finished{false};

    std::atomic<Status> cb_rc1{};
    std::atomic<Status> cb_rc2{};
    std::atomic<Status> cb_rc3{};
    std::atomic<Status> cb_rc4{};
    std::atomic<Status> cb_rc5{};
    [[maybe_unused]] reason_code rc1{};
    [[maybe_unused]] reason_code rc2{};
    [[maybe_unused]] reason_code rc3{};
    [[maybe_unused]] reason_code rc4{};
    [[maybe_unused]] reason_code rc5{};
    auto cb1 = [&cb_rc1, &rc1](Status rs, [[maybe_unused]] reason_code rc,
                               [[maybe_unused]] durability_marker_type dm) {
        cb_rc1.store(rs, std::memory_order_release);
        rc1 = rc;
    };
    auto cb2 = [&cb_rc2, &rc2](Status rs, [[maybe_unused]] reason_code rc,
                               [[maybe_unused]] durability_marker_type dm) {
        cb_rc2.store(rs, std::memory_order_release);
        rc2 = rc;
    };
    auto cb3 = [&cb_rc3, &rc3](Status rs, [[maybe_unused]] reason_code rc,
                               [[maybe_unused]] durability_marker_type dm) {
        cb_rc3.store(rs, std::memory_order_release);
        rc3 = rc;
    };
    auto cb4 = [&cb_rc4, &rc4](Status rs, [[maybe_unused]] reason_code rc,
                               [[maybe_unused]] durability_marker_type dm) {
        cb_rc4.store(rs, std::memory_order_release);
        rc4 = rc;
    };
    auto cb5 = [&cb_rc5, &rc5](Status rs, [[maybe_unused]] reason_code rc,
                               [[maybe_unused]] durability_marker_type dm) {
        cb_rc5.store(rs, std::memory_order_release);
        rc5 = rc;
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
    Token t5;
    ASSERT_OK(enter(t1));
    ASSERT_OK(enter(t2));
    ASSERT_OK(enter(t3));
    ASSERT_OK(enter(t4));
    ASSERT_OK(enter(t5));

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

    // t2 begin, write a
    if (t2_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t2, t2_type}));
    } else if (t2_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t2, t2_type, {sta}}));
        wait_epoch_update();
    } else {
        LOG(FATAL);
    }
    ASSERT_OK(upsert(t2, sta, "a", "2"));
    commit(t2, cb2);

    // t3 begin, read a
    if (t3_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t3, t3_type}));
    } else if (t3_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t3, t3_type}));
        wait_epoch_update();
    } else {
        LOG(FATAL);
    }
    ASSERT_OK(search_key(t3, sta, "a", buf));
    ASSERT_EQ(buf, "2");

    // t3 read u, commit
    ASSERT_OK(search_key(t3, stu, "u", buf));
    ASSERT_EQ(buf, "0");
    commit(t3, cb3);

    // t4 begin, write u, commit
    if (t4_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t4, t4_type}));
    } else if (t4_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t4, t4_type, {stu}}));
        wait_epoch_update();
    } else {
        LOG(FATAL);
    }
    ASSERT_OK(upsert(t4, stu, "u", "4"));
    commit(t4, cb4);

    // t5 begin, read u
    if (t5_type == transaction_type::SHORT) {
        ASSERT_OK(tx_begin({t5, t5_type}));
    } else if (t5_type == transaction_type::LONG) {
        ASSERT_OK(tx_begin({t5, t5_type}));
        wait_epoch_update();
    } else {
        LOG(FATAL);
    }
    ASSERT_OK(search_key(t5, stu, "u", buf));
    ASSERT_EQ(buf, "4");

    // t5 read x, commit
    if (t5_type == transaction_type::SHORT &&
        t1_type == transaction_type::LONG) {
        // fail case
        ASSERT_EQ(Status::ERR_CC, search_key(t5, stx, "x", buf));
        t5_was_finished = true;
    } else {
        // success case
        ASSERT_OK(search_key(t5, stx, "x", buf));
        ASSERT_EQ(buf, "0");
        commit(t5, cb5);
    }

    // t1 write x, commit
    ASSERT_OK(upsert(t1, stx, "x", "1"));
    commit(t1, cb1);

    // verify t1
    if (t1_can_commit) {
        ASSERT_EQ(cb_rc1, Status::OK);
    } else {
        ASSERT_EQ(cb_rc1, Status::ERR_CC);
    }

    // verify t2
    ASSERT_EQ(cb_rc2, Status::OK);

    // verify t3
    ASSERT_EQ(cb_rc3, Status::OK);

    // verify t4
    ASSERT_EQ(cb_rc4, Status::OK);

    // verify t5
    if (!t5_was_finished) {
        if (t5_can_commit) {
            ASSERT_EQ(cb_rc5, Status::OK);
        } else {
            ASSERT_EQ(cb_rc5, Status::ERR_CC);
        }
    }

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
    ASSERT_OK(leave(t3));
    ASSERT_OK(leave(t4));
    ASSERT_OK(leave(t5));
}

} // namespace shirakami::testing
