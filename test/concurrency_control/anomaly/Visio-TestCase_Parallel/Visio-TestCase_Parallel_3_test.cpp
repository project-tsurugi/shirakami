
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
                                  "TestCase_Parallel_3_test");
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
    // 2tx_3read_no_conflict
    // all ltx

    std::atomic<Status> cb_rc1{};
    std::atomic<Status> cb_rc2{};
    std::atomic<bool> t1_was_committed{false};
    std::atomic<bool> t2_was_committed{false};
    [[maybe_unused]] reason_code rc1{};
    [[maybe_unused]] reason_code rc2{};
    auto cb1 = [&cb_rc1, &rc1,
                &t1_was_committed](Status rs, [[maybe_unused]] reason_code rc,
                                   [[maybe_unused]] durability_marker_type dm) {
        cb_rc1.store(rs, std::memory_order_release);
        rc1 = rc;
        t1_was_committed = true;
    };
    auto cb2 = [&cb_rc2, &rc2,
                &t2_was_committed](Status rs, [[maybe_unused]] reason_code rc,
                                   [[maybe_unused]] durability_marker_type dm) {
        cb_rc2.store(rs, std::memory_order_release);
        rc2 = rc;
        t2_was_committed = true;
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
    ASSERT_OK(enter(t1));
    ASSERT_OK(enter(t2));

    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));
    ASSERT_OK(upsert(t1, sta, "a", "0"));
    ASSERT_OK(upsert(t1, stb, "b", "0"));
    ASSERT_OK(upsert(t1, stx, "x", "0"));
    ASSERT_OK(upsert(t1, sty, "y", "0"));
    ASSERT_OK(upsert(t1, stz, "z", "0"));
    ASSERT_OK(commit(t1));

    // test
    // t1 begin, t2 begin.
    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {stx, sty, stz}}));
    ASSERT_OK(tx_begin({t2, transaction_type::LONG, {sta, stb}}));
    wait_epoch_update();

    // t1 read x
    std::string buf{};
    ASSERT_OK(search_key(t1, stx, "x", buf));
    ASSERT_EQ(buf, "0");

    // t1 read y
    ASSERT_OK(search_key(t1, sty, "y", buf));
    ASSERT_EQ(buf, "0");

    // t1 read z
    ASSERT_OK(search_key(t1, stz, "z", buf));
    ASSERT_EQ(buf, "0");

    // t2 read z
    ASSERT_OK(search_key(t2, stz, "z", buf));
    ASSERT_EQ(buf, "0");

    // t2 read y
    ASSERT_OK(search_key(t2, sty, "y", buf));
    ASSERT_EQ(buf, "0");

    // t2 read x
    ASSERT_OK(search_key(t2, stx, "x", buf));
    ASSERT_EQ(buf, "0");

    // t1 write x
    ASSERT_OK(upsert(t1, stx, "x", "1"));

    // t1 read a
    ASSERT_OK(search_key(t1, sta, "a", buf));
    ASSERT_EQ(buf, "0");

    // t1 write y
    ASSERT_OK(upsert(t1, sty, "y", "1"));

    // t1 write z, commit
    ASSERT_OK(upsert(t1, stz, "z", "1"));
    commit(t1, cb1);

    // t2 write b
    ASSERT_OK(upsert(t2, stb, "b", "1"));

    // t2 write b
    ASSERT_OK(upsert(t2, stb, "b", "1"));

    // t2 write b
    ASSERT_OK(upsert(t2, stb, "b", "1"));

    // t2 write a, commit
    ASSERT_OK(upsert(t2, sta, "a", "1"));
    commit(t2, cb2);

    // verify
    ASSERT_OK(cb_rc1);
    ASSERT_EQ(Status::ERR_CC, cb_rc2);
    ASSERT_EQ(rc2, reason_code::CC_LTX_WRITE_COMMITTED_READ_PROTECTION);

    // read b
    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));
    ASSERT_OK(search_key(t1, stb, "b", buf));
    ASSERT_EQ(buf, "0");
    ASSERT_OK(commit(t1));

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
}

} // namespace shirakami::testing