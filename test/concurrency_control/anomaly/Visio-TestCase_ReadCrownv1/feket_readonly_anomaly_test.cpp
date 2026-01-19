
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/record.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class feket_readonly_anomaly_test : public ::testing::TestWithParam<transaction_options::transaction_type> {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-anomaly-"
                                  "read_crown-feket_readonly_anomaly_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

INSTANTIATE_TEST_SUITE_P(t3_type, feket_readonly_anomaly_test,
                         ::testing::Values(transaction_options::transaction_type::LONG,
                                           transaction_options::transaction_type::READ_ONLY));

TEST_P(feket_readonly_anomaly_test, all) { // NOLINT
    { GTEST_SKIP() << "LONG is not supported"; }
                                           // create table
    // ==========
    // prepare
    Storage stx{};
    Storage sty{};
    ASSERT_EQ(create_storage("x", stx), Status::OK);
    ASSERT_EQ(create_storage("y", sty), Status::OK);

    // enter
    std::array<Token, 4> s{};
    for (auto&& elem : s) { ASSERT_EQ(enter(elem), Status::OK); }

    // prepare page key value
    std::string x{"x"};
    std::string y{"y"};
    std::array<std::string, 4> var{"t0", "t1", "t2", "t3"};
    auto init_db = [&s, &var, x, y, stx, sty]() {
        epoch::set_perm_to_proc(epoch::ptp_init_val);
        ASSERT_EQ(Status::OK,
                  tx_begin({s.at(0),
                            transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, upsert(s.at(0), stx, x, var.at(0)));
        ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, var.at(0)));
        ASSERT_EQ(Status::OK, commit(s.at(0)));
    };
    init_db();
    std::string buf{};
    // ==========

    // ==========
    // note: o is occ, l is ltx, x is omit
    // anomaly
    // lol
    // safe_read
    // lol (last ltx is read only; ie. rtx)
    auto t3_type = GetParam();
    auto [t3_read_v, t3_commit_rc] = t3_type == transaction_options::transaction_type::LONG
            ? std::make_tuple(var.at(2), Status::ERR_CC)
            : std::make_tuple(var.at(0), Status::OK);

    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {sty}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), stx, x, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(2), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(2), stx, x, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(1), sty, y, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(2), stx, x, var.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(Status::OK, tx_begin({s.at(3), t3_type}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(3), sty, y, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(3), stx, x, buf));
    ASSERT_EQ(buf, t3_read_v);
    ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, var.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(t3_commit_rc, commit(s.at(3)));

    // verify
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(0), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, var.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, var.at(1));
    ASSERT_EQ(Status::OK, commit(s.at(0)));

    // cleanup
    init_db();

    // ==========

    // ==========
    // cleanup
    for (auto&& elem : s) { ASSERT_EQ(leave(elem), Status::OK); }
    // ==========
}

} // namespace shirakami::testing
