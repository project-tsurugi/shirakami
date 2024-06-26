
#include <xmmintrin.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <climits>
#include <mutex>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class search_upsert : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-hybrid-"
                "search_upsert-short_search_long_upsert_test");
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

TEST_F(search_upsert, short_search_find_valid_wp) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token ss{}; // short
    Token sb{}; // long
    ASSERT_EQ(enter(ss), Status::OK);
    ASSERT_EQ(enter(sb), Status::OK);
    // prepare data
    ASSERT_EQ(Status::OK,
              tx_begin({ss, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(ss, st, "", ""));
    ASSERT_EQ(Status::OK, commit(ss)); // NOLINT
    ASSERT_EQ(tx_begin({ss, transaction_options::transaction_type::SHORT}),
              Status::OK);
    ASSERT_EQ(tx_begin({sb, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(Status::ERR_CC, search_key(ss, st, "", vb));
    ASSERT_EQ(Status::OK, upsert(sb, st, "", ""));
    ASSERT_EQ(Status::OK, commit(sb)); // NOLINT
    ASSERT_EQ(leave(ss), Status::OK);
    ASSERT_EQ(leave(sb), Status::OK);
}

TEST_F(search_upsert, short_search_finish_before_valid_wp) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token ss{}; // short
    Token sb{}; // long
    ASSERT_EQ(enter(ss), Status::OK);
    ASSERT_EQ(enter(sb), Status::OK);
    // prepare data
    ASSERT_EQ(Status::OK,
              tx_begin({ss, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(ss, st, "", ""));
    ASSERT_EQ(Status::OK, commit(ss)); // NOLINT
    stop_epoch();
    ASSERT_EQ(tx_begin({ss}), Status::OK);
    ASSERT_EQ(tx_begin({sb, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(ss, st, "", vb));
    ASSERT_EQ(Status::OK, commit(ss)); // NOLINT
    resume_epoch();
    wait_epoch_update();
    ASSERT_EQ(Status::OK, upsert(sb, st, "", ""));
    ASSERT_EQ(Status::OK, commit(sb)); // NOLINT
    ASSERT_EQ(leave(ss), Status::OK);
    ASSERT_EQ(leave(sb), Status::OK);
}

TEST_F(search_upsert, short_search_finish_after_valid_wp) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token ss{}; // short
    Token sb{}; // long
    ASSERT_EQ(enter(ss), Status::OK);
    ASSERT_EQ(enter(sb), Status::OK);
    // prepare data
    ASSERT_EQ(Status::OK,
              tx_begin({ss, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(ss, st, "", ""));
    ASSERT_EQ(Status::OK, commit(ss)); // NOLINT
    // end prepare data
    // start test
    stop_epoch();
    ASSERT_EQ(tx_begin({ss}), Status::OK);
    ASSERT_EQ(tx_begin({sb, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(ss, st, "", vb));
    resume_epoch();
    wait_epoch_update();
    ASSERT_EQ(Status::ERR_CC, commit(ss)); // NOLINT
    // due to wp
    ASSERT_EQ(Status::OK, upsert(sb, st, "", ""));
    ASSERT_EQ(Status::OK, commit(sb)); // NOLINT
    // end test
    // cleanup test program
    ASSERT_EQ(leave(ss), Status::OK);
    ASSERT_EQ(leave(sb), Status::OK);
}

TEST_F(search_upsert, old_short_search_long_upsert_conflict) { // NOLINT
    /**
     * ltx1: w(x)
     * ltx2: r(x)w(y), find w(x) and do forewarding and conflict on stx
     * stx: r(y), e_ltx1 <= epoch < e_ltx2
     */
    Storage st_x{};
    Storage st_y{};
    ASSERT_EQ(create_storage("1", st_x), Status::OK);
    ASSERT_EQ(create_storage("2", st_y), Status::OK);
    std::string x{"x"};
    std::string y{"y"};
    {
        // prepare data
        Token s{};
        ASSERT_EQ(enter(s), Status::OK);
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, upsert(s, st_x, x, ""));
        ASSERT_EQ(Status::OK, upsert(s, st_y, y, ""));
        ASSERT_EQ(Status::OK, commit(s));
        LOG(INFO) << "short commit at " << epoch::get_global_epoch();
        ASSERT_EQ(leave(s), Status::OK);
    }
    {
        // test
        Token ltx1{};
        epoch::get_ep_mtx().lock();
        ASSERT_EQ(enter(ltx1), Status::OK);
        ASSERT_EQ(Status::OK,
                  tx_begin({ltx1,
                            transaction_options::transaction_type::LONG,
                            {st_x}}));
        epoch::set_perm_to_proc(1);
        epoch::get_ep_mtx().unlock();
        while (epoch::get_perm_to_proc() != 0) { _mm_pause(); }
        auto* ltx1s = static_cast<session*>(ltx1);
        LOG(INFO) << "ltx1's epoch: " << ltx1s->get_valid_epoch();
        Token stx{};
        ASSERT_EQ(enter(stx), Status::OK);
        ASSERT_EQ(
                Status::OK,
                tx_begin({stx, transaction_options::transaction_type::SHORT}));
        std::string buf{};
        ASSERT_EQ(Status::OK, search_key(stx, st_y, y, buf));
        ASSERT_EQ(Status::OK, commit(stx));
        epoch::set_perm_to_proc(epoch::ptp_init_val);

        // about ltx2
        Token ltx2{};
        ASSERT_EQ(enter(ltx2), Status::OK);
        ASSERT_EQ(Status::OK,
                  tx_begin({ltx2,
                            transaction_options::transaction_type::LONG,
                            {st_y}}));
        wait_epoch_update();
        ASSERT_EQ(Status::OK, search_key(ltx2, st_x, x, buf)); // may forwarding
        ASSERT_EQ(Status::OK, upsert(ltx2, st_y, y, ""));

        // about ltx1
        ASSERT_EQ(Status::OK, upsert(ltx1, st_x, x, ""));
        ASSERT_EQ(Status::OK, commit(ltx1));

        // about ltx2
        ASSERT_EQ(Status::ERR_CC, commit(ltx2));

        ASSERT_EQ(leave(stx), Status::OK);
        ASSERT_EQ(leave(ltx1), Status::OK);
        ASSERT_EQ(leave(ltx2), Status::OK);
    }
}

} // namespace shirakami::testing
