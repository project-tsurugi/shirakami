
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "test_tool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;



namespace shirakami::testing {

class Visio_TestCase_Parallel : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "anomaly-Visio-TestCase_Parallel-Visio-"
                                  "TestCase_Parallel_12_test");
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

TEST_F(Visio_TestCase_Parallel, test) { // NOLINT
    // parallel_read_crown
    // 3tx_3read_boundary_hop
    // all ltx

    std::atomic<Status> cb_rc1{};
    std::atomic<Status> cb_rc2{};
    std::atomic<Status> cb_rc3{};
    std::atomic<Status> cb_rc4{};
    std::atomic<Status> cb_rc5{};
    std::atomic<Status> cb_rc6{};
    std::atomic<Status> cb_rc7{};
    std::atomic<Status> cb_rc8{};
    std::atomic<bool> t1_was_committed{false};
    std::atomic<bool> t2_was_committed{false};
    std::atomic<bool> t3_was_committed{false};
    std::atomic<bool> t4_was_committed{false};
    std::atomic<bool> t5_was_committed{false};
    std::atomic<bool> t6_was_committed{false};
    std::atomic<bool> t7_was_committed{false};
    std::atomic<bool> t8_was_committed{false};
    [[maybe_unused]] reason_code rc1{};
    [[maybe_unused]] reason_code rc2{};
    [[maybe_unused]] reason_code rc3{};
    [[maybe_unused]] reason_code rc4{};
    [[maybe_unused]] reason_code rc5{};
    [[maybe_unused]] reason_code rc6{};
    [[maybe_unused]] reason_code rc7{};
    [[maybe_unused]] reason_code rc8{};
    [[maybe_unused]] auto cb1 =
            [&cb_rc1, &rc1,
             &t1_was_committed](Status rs, [[maybe_unused]] reason_code rc,
                                [[maybe_unused]] durability_marker_type dm) {
                cb_rc1.store(rs, std::memory_order_release);
                rc1 = rc;
                t1_was_committed = true;
            };
    [[maybe_unused]] auto cb2 =
            [&cb_rc2, &rc2,
             &t2_was_committed](Status rs, [[maybe_unused]] reason_code rc,
                                [[maybe_unused]] durability_marker_type dm) {
                cb_rc2.store(rs, std::memory_order_release);
                rc2 = rc;
                t2_was_committed = true;
            };
    [[maybe_unused]] auto cb3 =
            [&cb_rc3, &rc3,
             &t3_was_committed](Status rs, [[maybe_unused]] reason_code rc,
                                [[maybe_unused]] durability_marker_type dm) {
                cb_rc3.store(rs, std::memory_order_release);
                rc3 = rc;
                t3_was_committed = true;
            };
    [[maybe_unused]] auto cb4 =
            [&cb_rc4, &rc4,
             &t4_was_committed](Status rs, [[maybe_unused]] reason_code rc,
                                [[maybe_unused]] durability_marker_type dm) {
                cb_rc4.store(rs, std::memory_order_release);
                rc4 = rc;
                t4_was_committed = true;
            };
    [[maybe_unused]] auto cb5 =
            [&cb_rc5, &rc5,
             &t5_was_committed](Status rs, [[maybe_unused]] reason_code rc,
                                [[maybe_unused]] durability_marker_type dm) {
                cb_rc5.store(rs, std::memory_order_release);
                rc5 = rc;
                t5_was_committed = true;
            };
    [[maybe_unused]] auto cb6 =
            [&cb_rc6, &rc6,
             &t6_was_committed](Status rs, [[maybe_unused]] reason_code rc,
                                [[maybe_unused]] durability_marker_type dm) {
                cb_rc6.store(rs, std::memory_order_release);
                rc6 = rc;
                t6_was_committed = true;
            };
    [[maybe_unused]] auto cb7 =
            [&cb_rc7, &rc7,
             &t7_was_committed](Status rs, [[maybe_unused]] reason_code rc,
                                [[maybe_unused]] durability_marker_type dm) {
                cb_rc7.store(rs, std::memory_order_release);
                rc7 = rc;
                t7_was_committed = true;
            };
    [[maybe_unused]] auto cb8 =
            [&cb_rc8, &rc8,
             &t8_was_committed](Status rs, [[maybe_unused]] reason_code rc,
                                [[maybe_unused]] durability_marker_type dm) {
                cb_rc8.store(rs, std::memory_order_release);
                rc8 = rc;
                t8_was_committed = true;
            };

    // setup
    Storage sta{};
    Storage stb{};
    Storage stx{};
    Storage sty{};
    Storage stz{};
    ASSERT_OK(create_storage("a", sta));
    ASSERT_OK(create_storage("b", stb));
    ASSERT_OK(create_storage("x", stx));
    ASSERT_OK(create_storage("y", sty));
    ASSERT_OK(create_storage("z", stz));

    // prepare
    Token t1;
    Token t2;
    Token t3;
    Token t4;
    Token t5;
    Token t6;
    Token t7;
    Token t8;
    ASSERT_OK(enter(t1));
    ASSERT_OK(enter(t2));
    ASSERT_OK(enter(t3));
    ASSERT_OK(enter(t4));
    ASSERT_OK(enter(t5));
    ASSERT_OK(enter(t6));
    ASSERT_OK(enter(t7));
    ASSERT_OK(enter(t8));

    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));
    ASSERT_OK(upsert(t1, sta, "a", "0"));
    ASSERT_OK(upsert(t1, stb, "b", "0"));
    ASSERT_OK(upsert(t1, stx, "x", "0"));
    ASSERT_OK(upsert(t1, sty, "y", "0"));
    ASSERT_OK(upsert(t1, stz, "z", "0"));
    ASSERT_OK(commit(t1));

    // test
    // t1 2, 3, 4, 5, 6 begin.
    stop_epoch();
    ASSERT_OK(tx_begin({t1,
                        transaction_type::LONG,
                        {stb},
                        {{sta}, {stb, stx, sty, stz}}}));
    ASSERT_OK(tx_begin({t2, transaction_type::LONG, {}, {{stx}, {}}}));
    ASSERT_OK(tx_begin(
            {t3, transaction_type::LONG, {stb}, {{}, {stb, stx, sty, stz}}}));
    ASSERT_OK(tx_begin(
            {t4, transaction_type::LONG, {stx}, {{}, {stb, sty, stz}}}));
    LOG(INFO) << static_cast<session*>(t4)->get_long_tx_id();
    ASSERT_OK(tx_begin({t5, transaction_type::LONG, {stz}, {{}, {stb, sty}}}));
    ASSERT_OK(tx_begin({t6, transaction_type::LONG, {sty}, {{}, {stb}}}));
    resume_epoch();
    wait_epoch_update();

    // for read
    std::string buf{};

    // t1 read a
    ASSERT_OK(search_key(t1, sta, "a", buf));
    ASSERT_EQ(buf, "0");

    // t2 read x, commit
    ASSERT_OK(search_key(t2, stx, "x", buf));
    ASSERT_EQ(buf, "0");
    commit(t2, cb2);

    // t3 write b, commit
    ASSERT_OK(upsert(t3, stb, "b", "3"));
    commit(t3, cb3);

    // t4 write x, commit
    ASSERT_OK(upsert(t4, stx, "x", "4"));
    commit(t4, cb4);

    // t5 write z, commit
    ASSERT_OK(upsert(t5, stz, "z", "5"));
    commit(t5, cb5);

    // t6 write y, commit
    ASSERT_OK(upsert(t6, sty, "y", "6"));
    commit(t6, cb6);

    stop_epoch();

    // t7, 8 begin
    ASSERT_OK(tx_begin({t7, transaction_type::LONG}));
    ASSERT_OK(tx_begin({t8, transaction_type::LONG, {stb}}));

    resume_epoch();
    wait_epoch_update();

    // t7 read z
    ASSERT_OK(search_key(t7, stz, "z", buf));
    ASSERT_EQ(buf, "5");

    // t7 read y
    ASSERT_OK(search_key(t7, sty, "y", buf));
    ASSERT_EQ(buf, "6");

    // t7 read x
    ASSERT_OK(search_key(t7, stx, "x", buf));
    ASSERT_EQ(buf, "4");

    // t8 read b
    ASSERT_OK(search_key(t8, stb, "b", buf));
    ASSERT_EQ(buf, "3");

    // t7 read b, commit
    ASSERT_OK(search_key(t7, stb, "b", buf));
    ASSERT_EQ(buf, "3");
    commit(t7, cb7);

    // t8 write b, commit
    ASSERT_OK(upsert(t8, stb, "b", "8"));
    commit(t8, cb8);

    // t1 write b, commit
    ASSERT_OK(upsert(t1, stb, "b", "1"));
    commit(t1, cb1);

    // wait for verify
    while (!t1_was_committed || !t2_was_committed || !t3_was_committed ||
           !t4_was_committed || !t5_was_committed || !t6_was_committed ||
           !t7_was_committed || !t8_was_committed) {
        _mm_pause();
    }

    // verify
    ASSERT_OK(cb_rc1);
    ASSERT_OK(cb_rc2);
    ASSERT_OK(cb_rc3);
    ASSERT_OK(cb_rc4);
    ASSERT_OK(cb_rc5);
    ASSERT_OK(cb_rc6);
    ASSERT_EQ(Status::ERR_CC, cb_rc7);
    ASSERT_EQ(rc7, reason_code::CC_LTX_READ_UPPER_BOUND_VIOLATION);
    ASSERT_EQ(Status::ERR_CC, cb_rc8);
    ASSERT_EQ(rc8, reason_code::CC_LTX_READ_UPPER_BOUND_VIOLATION);

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
    ASSERT_OK(leave(t3));
    ASSERT_OK(leave(t4));
    ASSERT_OK(leave(t5));
    ASSERT_OK(leave(t6));
    ASSERT_OK(leave(t7));
    ASSERT_OK(leave(t8));
}

} // namespace shirakami::testing