
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "test_tool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class Visio_TestCase_Parallel : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "anomaly-Visio-TestCase_Parallel-Visio-"
                                  "TestCase_Parallel_11_test");
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

TEST_F(Visio_TestCase_Parallel, test_1_without_read_area) { // NOLINT
    // 2tx_3read_single_boundary
    // all ltx

    std::atomic<Status> cb_rc1{};
    std::atomic<Status> cb_rc2{};
    std::atomic<Status> cb_rc3{};
    std::atomic<bool> t1_was_committed{false};
    std::atomic<bool> t2_was_committed{false};
    std::atomic<bool> t3_was_committed{false};
    [[maybe_unused]] reason_code rc1{};
    [[maybe_unused]] reason_code rc2{};
    [[maybe_unused]] reason_code rc3{};
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
    auto cb3 = [&cb_rc3, &rc3,
                &t3_was_committed](Status rs, [[maybe_unused]] reason_code rc,
                                   [[maybe_unused]] durability_marker_type dm) {
        cb_rc3.store(rs, std::memory_order_release);
        rc3 = rc;
        t3_was_committed = true;
    };

    // setup
    Storage stb{};
    Storage stx{};
    Storage sty{};
    Storage stz{};
    ASSERT_OK(create_storage("b", stb));
    ASSERT_OK(create_storage("x", stx));
    ASSERT_OK(create_storage("y", sty));
    ASSERT_OK(create_storage("z", stz));

    // prepare
    Token t1;
    Token t2;
    Token t3;
    ASSERT_OK(enter(t1));
    ASSERT_OK(enter(t2));
    ASSERT_OK(enter(t3));

    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));
    ASSERT_OK(upsert(t1, stb, "b", "0"));
    ASSERT_OK(upsert(t1, stx, "x", "0"));
    ASSERT_OK(upsert(t1, sty, "y", "0"));
    ASSERT_OK(upsert(t1, stz, "z", "0"));
    ASSERT_OK(commit(t1));

    // test
    // t1 begin, read b
    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {stb}}));
    wait_epoch_update();
    std::string buf{};
    ASSERT_OK(search_key(t1, stb, "b", buf));
    ASSERT_EQ(buf, "0");

    // t2 begin, read x
    ASSERT_OK(tx_begin({t2, transaction_type::LONG, {stx}}));
    wait_epoch_update();
    ASSERT_OK(search_key(t2, stx, "x", buf));
    ASSERT_EQ(buf, "0");

    // t2 write x and commit
    ASSERT_OK(upsert(t2, stx, "x", "2"));
    commit(t2, cb2);

    // t3 begin, read z
    ASSERT_OK(tx_begin({t3, transaction_type::LONG}));
    wait_epoch_update();
    ASSERT_OK(search_key(t3, stz, "z", buf));
    ASSERT_EQ(buf, "0");
    // t3 read y
    ASSERT_OK(search_key(t3, sty, "y", buf));
    ASSERT_EQ(buf, "0");
    // t3 read x
    ASSERT_OK(search_key(t3, stx, "x", buf));
    // t2 は t1 を read wait して生き残っているため、t2 の wp を観測して前置を考慮
    ASSERT_EQ(buf, "2");
    LOG(INFO) << static_cast<session*>(t3)->get_read_version_max_epoch();
    // t3 read b, commit
    ASSERT_OK(search_key(t3, stb, "b", buf));
    // t1 の wp を観測して前置を考慮
    LOG(INFO) << static_cast<session*>(t3)->get_read_version_max_epoch();
    LOG(INFO) << static_cast<session*>(t3)->get_long_tx_id();
    commit(t3, cb3);

    // t1 write b, commit
    ASSERT_OK(upsert(t1, stb, "b", "1"));
    commit(t1, cb1);

    // verify t1
    while (!t1_was_committed) { _mm_pause(); }
    ASSERT_EQ(cb_rc1, Status::OK);

    // verify t2
    while (!t2_was_committed) { _mm_pause(); }
    ASSERT_EQ(cb_rc2, Status::OK);

    // verify t3
    while (!t3_was_committed) { _mm_pause(); }
    ASSERT_EQ(cb_rc3, Status::ERR_CC);
    ASSERT_EQ(rc3, reason_code::CC_LTX_READ_UPPER_BOUND_VIOLATION);

    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
    ASSERT_OK(leave(t3));
}

} // namespace shirakami::testing