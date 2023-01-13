
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/record.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class write_conflict_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-function-"
                                  "false_positive-write_conflict_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(write_conflict_test, all) { // NOLINT
                                   // create table
    // ==========
    // prepare
    Storage sta{};
    Storage stx{};
    Storage sty{};
    Storage stz{};
    ASSERT_EQ(create_storage("a", sta), Status::OK);
    ASSERT_EQ(create_storage("x", stx), Status::OK);
    ASSERT_EQ(create_storage("y", sty), Status::OK);
    ASSERT_EQ(create_storage("z", stz), Status::OK);

    // enter
    std::array<Token, 4> s{};
    for (auto&& elem : s) { ASSERT_EQ(enter(elem), Status::OK); }

    // prepare page key value
    std::string a{"a"};
    std::string x{"x"};
    std::string y{"y"};
    std::string z{"z"};
    std::array<std::string, 4> var{"t0", "t1", "t2", "t3"};
    auto init_db = [&s, &var, a, x, y, z, sta, stx, sty, stz]() {
        epoch::set_perm_to_proc(epoch::ptp_init_val);
        ASSERT_EQ(Status::OK, upsert(s.at(0), sta, a, var.at(0)));
        ASSERT_EQ(Status::OK, upsert(s.at(0), stx, x, var.at(0)));
        ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, var.at(0)));
        ASSERT_EQ(Status::OK, upsert(s.at(0), stz, z, var.at(0)));
        ASSERT_EQ(Status::OK, commit(s.at(0)));
    };
    init_db();
    std::string buf{};
    // ==========

    // ==========
    // note: o is occ, l is ltx
    // 3occ
    // ooo
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(1), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(1), stx, x, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(2), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, var.at(1)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(3), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(3), sty, y, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, var.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), stz, z, var.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), stx, x, var.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, var.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, var.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, var.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, var.at(3));
    ASSERT_EQ(Status::OK, commit(s.at(0)));

    // cleanup
    init_db();

    // 2occ1ltx
    // ool
    ASSERT_EQ(Status::OK, tx_begin({s.at(3),
                                    transaction_options::transaction_type::LONG,
                                    {stz}}));
    wait_epoch_update();
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(1), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(1), stx, x, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(2), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CC, search_key(s.at(2), stz, z, buf));
    //ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, var.at(1)));
    ASSERT_EQ(Status::OK, search_key(s.at(3), sty, y, buf));
    ASSERT_EQ(buf, var.at(0));
    //ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, var.at(2)));
    //ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), stz, z, var.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), stx, x, var.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, var.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, var.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, var.at(3));
    ASSERT_EQ(Status::OK, commit(s.at(0)));

    // cleanup
    init_db();

    // 1occ1ltx1occ
    // olo
    ASSERT_EQ(Status::OK, tx_begin({s.at(2),
                                    transaction_options::transaction_type::LONG,
                                    {sta}}));
    wait_epoch_update();
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(1), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(1), stx, x, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, var.at(1)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(3), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(3), sty, y, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, var.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), stz, z, var.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), stx, x, var.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, var.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, var.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, var.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, var.at(3));
    ASSERT_EQ(Status::OK, commit(s.at(0)));

    // cleanup
    init_db();

    // 1ltx2occ
    // loo
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {stx, sty}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), stx, x, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(2), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, var.at(1)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(3), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CC, search_key(s.at(3), sty, y, buf));
    //ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, var.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    //ASSERT_EQ(Status::OK, upsert(s.at(3), stz, z, var.at(3)));
    //ASSERT_EQ(Status::OK, commit(s.at(3)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), stx, x, var.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, var.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, var.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, var.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, commit(s.at(0)));

    // cleanup
    init_db();

    // 1occ2ltx
    // oll
    ASSERT_EQ(Status::OK, tx_begin({s.at(2),
                                    transaction_options::transaction_type::LONG,
                                    {sta}}));
    ASSERT_EQ(Status::OK, tx_begin({s.at(3),
                                    transaction_options::transaction_type::LONG,
                                    {stz}}));
    wait_epoch_update();
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(1), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(1), stx, x, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, var.at(1)));
    ASSERT_EQ(Status::OK, search_key(s.at(3), sty, y, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, var.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), stz, z, var.at(3)));
    ASSERT_EQ(Status::ERR_CC, commit(s.at(3)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), stx, x, var.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, var.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, var.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, var.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, commit(s.at(0)));

    // cleanup
    init_db();

    // 1ltx1occ1ltx
    // lol
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {stx, sty}}));
    ASSERT_EQ(Status::OK, tx_begin({s.at(3),
                                    transaction_options::transaction_type::LONG,
                                    {stz}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), stx, x, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(2), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CC, search_key(s.at(2), stz, z, buf));
    //ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, var.at(1)));
    ASSERT_EQ(Status::OK, search_key(s.at(3), sty, y, buf));
    ASSERT_EQ(buf, var.at(0));
    //ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, var.at(2)));
    //ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), stz, z, var.at(3)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), stx, x, var.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, var.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, var.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, var.at(3));
    ASSERT_EQ(Status::OK, commit(s.at(0)));

    // cleanup
    init_db();

    // 2ltx1occ
    // llo
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {stx, sty},
                                    {{stx}, {sta}}}));
    ASSERT_EQ(Status::OK, tx_begin({s.at(2),
                                    transaction_options::transaction_type::LONG,
                                    {sta}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), stx, x, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, var.at(1)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(3), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CC, search_key(s.at(3), sty, y, buf));
    //ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, var.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    //ASSERT_EQ(Status::OK, upsert(s.at(3), stz, z, var.at(3)));
    //ASSERT_EQ(Status::OK, commit(s.at(3)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), stx, x, var.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, var.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, var.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, var.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, commit(s.at(0)));

    // cleanup
    init_db();

    // 3ltx
    // lll
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {stx, sty},
                                    {{stx}, {sta}}}));
    ASSERT_EQ(Status::OK, tx_begin({s.at(2),
                                    transaction_options::transaction_type::LONG,
                                    {sta}}));
    ASSERT_EQ(Status::OK, tx_begin({s.at(3),
                                    transaction_options::transaction_type::LONG,
                                    {stz}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), stx, x, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, var.at(1)));
    ASSERT_EQ(Status::OK, search_key(s.at(3), sty, y, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, var.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), stz, z, var.at(3)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), stx, x, var.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(Status::ERR_CC, commit(s.at(3)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, var.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, var.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, var.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, var.at(0));
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