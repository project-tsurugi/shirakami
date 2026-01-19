
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
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class sequent_4t_cycled_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-anomaly-"
                                  "write_crown-sequent_4t_cycled_test");
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

TEST_F(sequent_4t_cycled_test, all) { // NOLINT
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
    std::array<Token, 5> s{};
    for (auto&& elem : s) { ASSERT_EQ(enter(elem), Status::OK); }

    // prepare page key value
    std::string a{"a"};
    std::string x{"x"};
    std::string y{"y"};
    std::string z{"z"};
    std::array<std::string, 5> v{"t0", "t1", "t2", "t3", "t4"};
    auto init_db = [&s, &v, a, x, y, z, sta, stx, sty, stz]() {
        epoch::set_perm_to_proc(epoch::ptp_init_val);
        ASSERT_EQ(Status::OK,
                  tx_begin({s.at(0),
                            transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, upsert(s.at(0), sta, a, v.at(0)));
        ASSERT_EQ(Status::OK, upsert(s.at(0), stx, x, v.at(0)));
        ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, v.at(0)));
        ASSERT_EQ(Status::OK, upsert(s.at(0), stz, z, v.at(0)));
        ASSERT_EQ(Status::OK, commit(s.at(0)));
    };
    init_db();
    std::string buf{};
    // ==========

    // ==========
    // note: o is occ, l is ltx
    // test case 1
    // oooo
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(1), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(1), stx, x, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, v.at(1)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sta, a, v.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(2), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(2), sty, y, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, upsert(s.at(2), stz, z, v.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, v.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(3), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(3), stz, z, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, upsert(s.at(3), sta, a, v.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(4), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(4), sta, a, buf));
    ASSERT_EQ(buf, v.at(3));
    ASSERT_EQ(Status::OK, upsert(s.at(4), stx, x, v.at(4)));
    ASSERT_EQ(Status::OK, upsert(s.at(4), sta, a, v.at(4)));
    ASSERT_EQ(Status::OK, commit(s.at(4)));

    // verify
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(0), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, v.at(4));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(4));
    ASSERT_EQ(Status::OK, commit(s.at(0)));

    // cleanup
    init_db();

#if 0
    // test case 2
    // oool
    ASSERT_EQ(Status::OK, tx_begin({s.at(4),
                                    transaction_options::transaction_type::LONG,
                                    {stx, sta}}));
    wait_epoch_update();
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(1), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CC, search_key(s.at(1), stx, x, buf));
    //ASSERT_EQ(buf, v.at(0));
    //ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, v.at(1)));
    //ASSERT_EQ(Status::OK, upsert(s.at(1), sta, a, v.at(1)));
    //ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(2), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(2), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(2), stz, z, v.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, v.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(3), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(3), stz, z, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, upsert(s.at(3), sta, a, v.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));
    ASSERT_EQ(Status::OK, search_key(s.at(4), sta, a, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(4), stx, x, v.at(4)));
    ASSERT_EQ(Status::OK, upsert(s.at(4), sta, a, v.at(4)));
    ASSERT_EQ(Status::OK, commit(s.at(4)));

    // verify
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(0), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, v.at(4));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(3));
    ASSERT_EQ(Status::OK, commit(s.at(0)));

    // cleanup
    init_db();

    // test case 3
    // oolo
    ASSERT_EQ(Status::OK, tx_begin({s.at(3),
                                    transaction_options::transaction_type::LONG,
                                    {sta}}));
    wait_epoch_update();
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(1), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(1), stx, x, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, v.at(1)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sta, a, v.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(2), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(2), sty, y, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, upsert(s.at(2), stz, z, v.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, v.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(Status::OK, search_key(s.at(3), stz, z, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(3), sta, a, v.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(4), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(4), sta, a, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, upsert(s.at(4), stx, x, v.at(4)));
    ASSERT_EQ(Status::OK, upsert(s.at(4), sta, a, v.at(4)));
    ASSERT_EQ(Status::OK, commit(s.at(4)));

    // verify
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(0), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, v.at(4));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(4));
    ASSERT_EQ(Status::OK, commit(s.at(0)));

    // cleanup
    init_db();

    // test case 4
    // oloo
    ASSERT_EQ(Status::OK, tx_begin({s.at(2),
                                    transaction_options::transaction_type::LONG,
                                    {stz, sta}}));
    wait_epoch_update();
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(1), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(1), stx, x, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, v.at(1)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sta, a, v.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(Status::OK, search_key(s.at(2), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(2), stz, z, v.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, v.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(3), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(3), stz, z, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, upsert(s.at(3), sta, a, v.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(4), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(4), sta, a, buf));
    ASSERT_EQ(buf, v.at(3));
    ASSERT_EQ(Status::OK, upsert(s.at(4), stx, x, v.at(4)));
    ASSERT_EQ(Status::OK, upsert(s.at(4), sta, a, v.at(4)));
    ASSERT_EQ(Status::OK, commit(s.at(4)));

    // verify
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(0), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, v.at(4));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(4));
    ASSERT_EQ(Status::OK, commit(s.at(0)));

    // cleanup
    init_db();

    // test case 5
    // looo
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {sty, sta}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), stx, x, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, v.at(1)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sta, a, v.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(2), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(2), sty, y, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, upsert(s.at(2), stz, z, v.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, v.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(3), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(3), stz, z, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, upsert(s.at(3), sta, a, v.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(4), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(4), sta, a, buf));
    ASSERT_EQ(buf, v.at(3));
    ASSERT_EQ(Status::OK, upsert(s.at(4), stx, x, v.at(4)));
    ASSERT_EQ(Status::OK, upsert(s.at(4), sta, a, v.at(4)));
    ASSERT_EQ(Status::OK, commit(s.at(4)));

    // verify
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(0), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, v.at(4));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(4));
    ASSERT_EQ(Status::OK, commit(s.at(0)));

    // cleanup
    init_db();

    // test case 6
    // ooll
    stop_epoch();
    ASSERT_EQ(Status::OK, tx_begin({s.at(3),
                                    transaction_options::transaction_type::LONG,
                                    {sta}}));
    ASSERT_EQ(Status::OK, tx_begin({s.at(4),
                                    transaction_options::transaction_type::LONG,
                                    {stx, sta}}));
    resume_epoch();
    wait_epoch_update();
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(1), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CC, search_key(s.at(1), stx, x, buf));
    //ASSERT_EQ(buf, v.at(0));
    //ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, v.at(1)));
    //ASSERT_EQ(Status::OK, upsert(s.at(1), sta, a, v.at(1)));
    //ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(2), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(2), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(2), stz, z, v.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, v.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(Status::OK, search_key(s.at(3), stz, z, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(3), sta, a, v.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));
    ASSERT_EQ(Status::OK, search_key(s.at(4), sta, a, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(4), stx, x, v.at(4)));
    ASSERT_EQ(Status::OK, upsert(s.at(4), sta, a, v.at(4)));
    ASSERT_EQ(Status::OK, commit(s.at(4)));

    // verify
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(0), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, v.at(4));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, commit(s.at(0)));

    // cleanup
    init_db();

    // test case 7
    // olol
    stop_epoch();
    ASSERT_EQ(Status::OK, tx_begin({s.at(2),
                                    transaction_options::transaction_type::LONG,
                                    {stz, sta}}));
    ASSERT_EQ(Status::OK, tx_begin({s.at(4),
                                    transaction_options::transaction_type::LONG,
                                    {stx, sta}}));
    resume_epoch();
    wait_epoch_update();
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(1), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CC, search_key(s.at(1), stx, x, buf));
    //ASSERT_EQ(buf, v.at(0));
    //ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, v.at(1)));
    //ASSERT_EQ(Status::OK, upsert(s.at(1), sta, a, v.at(1)));
    //ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(Status::OK, search_key(s.at(2), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(2), stz, z, v.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, v.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(3), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(3), stz, z, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, upsert(s.at(3), sta, a, v.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));
    ASSERT_EQ(Status::OK, search_key(s.at(4), sta, a, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(4), stx, x, v.at(4)));
    ASSERT_EQ(Status::OK, upsert(s.at(4), sta, a, v.at(4)));
    ASSERT_EQ(Status::OK, commit(s.at(4)));

    // verify
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(0), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, v.at(4));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(3));
    ASSERT_EQ(Status::OK, commit(s.at(0)));

    // cleanup
    init_db();

    // test case 8
    // lool
    stop_epoch();
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {sty, sta}}));
    ASSERT_EQ(Status::OK, tx_begin({s.at(4),
                                    transaction_options::transaction_type::LONG,
                                    {stx, sta}}));
    resume_epoch();
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), stx, x, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, v.at(1)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sta, a, v.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(2), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(2), sty, y, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, upsert(s.at(2), stz, z, v.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, v.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(3), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(3), stz, z, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, upsert(s.at(3), sta, a, v.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));
    ASSERT_EQ(Status::OK, search_key(s.at(4), sta, a, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(4), stx, x, v.at(4)));
    ASSERT_EQ(Status::OK, upsert(s.at(4), sta, a, v.at(4)));
    ASSERT_EQ(Status::ERR_CC, commit(s.at(4))); // false positive

    // verify
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(0), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(3));
    ASSERT_EQ(Status::OK, commit(s.at(0)));

    // cleanup
    init_db();
#endif

    // ==========

    // ==========
    // cleanup
    for (auto&& elem : s) { ASSERT_EQ(leave(elem), Status::OK); }
    // ==========
}

} // namespace shirakami::testing
