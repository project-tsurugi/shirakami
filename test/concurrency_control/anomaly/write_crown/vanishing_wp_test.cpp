
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
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class vanishing_wp_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-vanishing_wp_test");
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

TEST_F(vanishing_wp_test, all) { // NOLINT
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
    std::array<std::string, 4> v{"t1", "t2", "t3", "t4"};
    auto init_db = [&s, a, x, y, z, sta, stx, sty, stz]() {
        ASSERT_EQ(Status::OK, upsert(s.at(0), sta, a, ""));
        ASSERT_EQ(Status::OK, upsert(s.at(0), stx, x, ""));
        ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, ""));
        ASSERT_EQ(Status::OK, upsert(s.at(0), stz, z, ""));
        ASSERT_EQ(Status::OK, commit(s.at(0)));
    };
    init_db();
    // ==========

    // ==========
    // test case 1
    LOG(INFO) << "start 1";
    std::string buf{};
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(0), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(1), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(1), sty, y, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, v.at(0)));
    ASSERT_EQ(Status::OK, commit(s.at(0)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(2), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(1), stz, z, v.at(1)));
    ASSERT_EQ(Status::ERR_CC, commit(s.at(1)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(3), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(3), sta, a, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, v.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), stx, x, v.at(3)));
    ASSERT_EQ(Status::ERR_CC, commit(s.at(3)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");

    // cleanup
    init_db();

    // test case 2
    LOG(INFO) << "start 2";
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(0), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(1), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(1), sty, y, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, v.at(0)));
    ASSERT_EQ(Status::OK, commit(s.at(0)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(2), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(1), stz, z, v.at(1)));
    ASSERT_EQ(Status::ERR_CC, commit(s.at(1)));
    ASSERT_EQ(Status::OK, tx_begin({s.at(3),
                                    transaction_options::transaction_type::LONG,
                                    {stx}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(3), sta, a, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, v.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), stx, x, v.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, v.at(3));

    // cleanup
    init_db();

    // test case 3
    LOG(INFO) << "start 3";
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(0), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(1), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(1), sty, y, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, v.at(0)));
    ASSERT_EQ(Status::OK, commit(s.at(0)));
    ASSERT_EQ(Status::OK, tx_begin({s.at(2),
                                    transaction_options::transaction_type::LONG,
                                    {sta}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(1), stz, z, v.at(1)));
    ASSERT_EQ(Status::ERR_CC, commit(s.at(1)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(3), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CC, search_key(s.at(3), sta, a, buf));
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, v.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");

    // cleanup
    init_db();

    // test case 4
    LOG(INFO) << "start 4";
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(0), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {stz}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), sty, y, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, v.at(0)));
    ASSERT_EQ(Status::OK, commit(s.at(0)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(2), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CC, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(Status::OK, upsert(s.at(1), stz, z, v.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(3), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(3), sta, a, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(3), stx, x, v.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, v.at(3));

    // cleanup
    init_db();

    // test case 5
    LOG(INFO) << "start 5";
    ASSERT_EQ(Status::OK, tx_begin({s.at(0),
                                    transaction_options::transaction_type::LONG,
                                    {sty}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(1), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CC, search_key(s.at(1), sty, y, buf));
    ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, v.at(0)));
    ASSERT_EQ(Status::OK, commit(s.at(0)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(2), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(3), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(3), sta, a, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, v.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), stx, x, v.at(3)));
    ASSERT_EQ(Status::ERR_CC, commit(s.at(3)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");

    // cleanup
    init_db();

    // test case 6
    LOG(INFO) << "start 6";
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(0), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(1), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(1), sty, y, buf));
    ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, v.at(0)));
    ASSERT_EQ(Status::OK, commit(s.at(0)));
    ASSERT_EQ(Status::OK, tx_begin({s.at(2),
                                    transaction_options::transaction_type::LONG,
                                    {sta}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(1), stz, z, v.at(1)));
    ASSERT_EQ(Status::ERR_CC, commit(s.at(1)));
    ASSERT_EQ(Status::OK, tx_begin({s.at(3),
                                    transaction_options::transaction_type::LONG,
                                    {stx}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(3), sta, a, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, v.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), stx, x, v.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, v.at(3));

    // cleanup
    init_db();

    // test case 7
    LOG(INFO) << "start 7";
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(0), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {stz}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), sty, y, buf));
    ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, v.at(0)));
    ASSERT_EQ(Status::OK, commit(s.at(0)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(2), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CC, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(Status::OK, upsert(s.at(1), stz, z, v.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(Status::OK, tx_begin({s.at(3),
                                    transaction_options::transaction_type::LONG,
                                    {stx}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(3), sta, a, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(3), stx, x, v.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, v.at(3));

    // cleanup
    init_db();

    // test case 8
    LOG(INFO) << "start 8";
    ASSERT_EQ(Status::OK, tx_begin({s.at(0),
                                    transaction_options::transaction_type::LONG,
                                    {sty}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(1), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CC, search_key(s.at(1), sty, y, buf));
    ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, v.at(0)));
    ASSERT_EQ(Status::OK, commit(s.at(0)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(2), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, tx_begin({s.at(3),
                                    transaction_options::transaction_type::LONG,
                                    {stx}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(3), sta, a, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, v.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), stx, x, v.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, v.at(3));

    // cleanup
    init_db();

    // test case 9
    LOG(INFO) << "start 9";
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(0), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {stz}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), sty, y, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, v.at(0)));
    ASSERT_EQ(Status::OK, commit(s.at(0)));
    ASSERT_EQ(Status::OK, tx_begin({s.at(2),
                                    transaction_options::transaction_type::LONG,
                                    {sta}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(1), stz, z, v.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(3), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CC, search_key(s.at(3), sta, a, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, v.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");

    // cleanup
    init_db();

    // test case 10
    LOG(INFO) << "start 10";
    ASSERT_EQ(Status::OK, tx_begin({s.at(0),
                                    transaction_options::transaction_type::LONG,
                                    {sty}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(1), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CC, search_key(s.at(1), sty, y, buf));
    ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, v.at(0)));
    ASSERT_EQ(Status::OK, commit(s.at(0)));
    ASSERT_EQ(Status::OK, tx_begin({s.at(2),
                                    transaction_options::transaction_type::LONG,
                                    {sta}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(3), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CC, search_key(s.at(3), sta, a, buf));
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, v.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");

    // cleanup
    init_db();

    // test case 11
    LOG(INFO) << "start 11";
    ASSERT_EQ(Status::OK, tx_begin({s.at(0),
                                    transaction_options::transaction_type::LONG,
                                    {sty}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {stz}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), sty, y, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, v.at(0)));
    ASSERT_EQ(Status::OK, commit(s.at(0)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(2), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CC, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(Status::OK, upsert(s.at(1), stz, z, v.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(3), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(3), sta, a, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(3), stx, x, v.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, v.at(3));

    // cleanup
    init_db();

    // test case 12
    LOG(INFO) << "start 12";
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(0), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {stz}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), sty, y, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, v.at(0)));
    ASSERT_EQ(Status::OK, commit(s.at(0)));
    ASSERT_EQ(Status::OK, tx_begin({s.at(2),
                                    transaction_options::transaction_type::LONG,
                                    {sta}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(1), stz, z, v.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(Status::OK, tx_begin({s.at(3),
                                    transaction_options::transaction_type::LONG,
                                    {stx}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(3), sta, a, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, v.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), stx, x, v.at(3)));
    ASSERT_EQ(Status::ERR_CC, commit(s.at(3)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");

    // cleanup
    init_db();

    // test case 13
    LOG(INFO) << "start 13";
    ASSERT_EQ(Status::OK, tx_begin({s.at(0),
                                    transaction_options::transaction_type::LONG,
                                    {sty}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(1), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CC, search_key(s.at(1), sty, y, buf));
    ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, v.at(0)));
    ASSERT_EQ(Status::OK, commit(s.at(0)));
    ASSERT_EQ(Status::OK, tx_begin({s.at(2),
                                    transaction_options::transaction_type::LONG,
                                    {sta}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, tx_begin({s.at(3),
                                    transaction_options::transaction_type::LONG,
                                    {stx}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(3), sta, a, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, v.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), stx, x, v.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, v.at(3));

    // cleanup
    init_db();

    // test case 14
    LOG(INFO) << "start 14";
    ASSERT_EQ(Status::OK, tx_begin({s.at(0),
                                    transaction_options::transaction_type::LONG,
                                    {sty}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {stz}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), sty, y, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, v.at(0)));
    ASSERT_EQ(Status::OK, commit(s.at(0)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(2), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CC, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(Status::OK, upsert(s.at(1), stz, z, v.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(Status::OK, tx_begin({s.at(3),
                                    transaction_options::transaction_type::LONG,
                                    {stx}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(3), sta, a, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(3), stx, x, v.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, v.at(3));

    // cleanup
    init_db();

    // test case 15
    LOG(INFO) << "start 15";
    ASSERT_EQ(Status::OK, tx_begin({s.at(0),
                                    transaction_options::transaction_type::LONG,
                                    {sty}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {stz}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), sty, y, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, v.at(0)));
    ASSERT_EQ(Status::OK, commit(s.at(0)));
    ASSERT_EQ(Status::OK, tx_begin({s.at(2),
                                    transaction_options::transaction_type::LONG,
                                    {sta}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(1), stz, z, v.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(3), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CC, search_key(s.at(3), sta, a, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, v.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");

    // cleanup
    init_db();

    // test case 16
    LOG(INFO) << "start 16";
    ASSERT_EQ(Status::OK, tx_begin({s.at(0),
                                    transaction_options::transaction_type::LONG,
                                    {sty}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {stz}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), sty, y, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, v.at(0)));
    ASSERT_EQ(Status::OK, commit(s.at(0)));
    ASSERT_EQ(Status::OK, tx_begin({s.at(2),
                                    transaction_options::transaction_type::LONG,
                                    {sta}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(2), stz, z, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(1), stz, z, v.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(Status::OK, tx_begin({s.at(3),
                                    transaction_options::transaction_type::LONG,
                                    {stx}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(3), sta, a, buf));
    ASSERT_EQ(buf, "");
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, v.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), stx, x, v.at(3)));
    ASSERT_EQ(Status::ERR_CC, commit(s.at(3)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, "");

    // cleanup
    init_db();

    // ==========

    // ==========
    // cleanup
    for (auto&& elem : s) { ASSERT_EQ(leave(elem), Status::OK); }
    // ==========
}

} // namespace shirakami::testing