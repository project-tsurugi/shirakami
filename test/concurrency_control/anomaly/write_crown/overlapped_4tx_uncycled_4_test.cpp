
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class overlapped_4tx_uncycled_4_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-anomaly-"
                                  "write_crown-overlapped_4tx_uncycled_4_test");
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

TEST_F(overlapped_4tx_uncycled_4_test, all) { // NOLINT
                                              // create table
    // ==========
    // prepare
    Storage sta{};
    Storage stb{};
    Storage stx{};
    Storage sty{};
    Storage stz{};
    ASSERT_EQ(create_storage("a", sta), Status::OK);
    ASSERT_EQ(create_storage("b", stb), Status::OK);
    ASSERT_EQ(create_storage("x", stx), Status::OK);
    ASSERT_EQ(create_storage("y", sty), Status::OK);
    ASSERT_EQ(create_storage("z", stz), Status::OK);

    // enter
    std::array<Token, 5> s{};
    for (auto&& elem : s) { ASSERT_EQ(enter(elem), Status::OK); }

    // prepare page key value
    std::string a{"a"};
    std::string b{"b"};
    std::string x{"x"};
    std::string y{"y"};
    std::string z{"z"};
    std::array<std::string, 5> v{"t0", "t1", "t2", "t3", "t4"};
    auto init_db = [&s, &v, a, b, x, y, z, sta, stb, stx, sty, stz]() {
        epoch::set_perm_to_proc(epoch::ptp_init_val);
        ASSERT_EQ(Status::OK, upsert(s.at(0), sta, a, v.at(0)));
        ASSERT_EQ(Status::OK, upsert(s.at(0), stb, b, v.at(0)));
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
    // test case 13
    // loll
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {sty, stb},
                                    {{}, {sta, stb}}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), stx, x, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(2), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CONFLICT_ON_WRITE_PRESERVE,
              search_key(s.at(2), sty, y, buf));
    ASSERT_EQ(Status::OK, tx_begin({s.at(3),
                                    transaction_options::transaction_type::LONG,
                                    {sta, stb}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(3), stz, z, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, tx_begin({s.at(4),
                                    transaction_options::transaction_type::LONG,
                                    {stb}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(4), sta, a, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(4), stb, b, v.at(4)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), sta, a, v.at(3)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), stb, b, v.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, v.at(1)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), stb, b, v.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    // t4 wait t1 for read-x
    ASSERT_EQ(Status::OK, commit(s.at(4)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(3));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stb, b, buf));
    ASSERT_EQ(buf, v.at(3));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, commit(s.at(0)));

    // cleanup
    init_db();

    // test case 14
    // llol
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {sty, stb}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), stx, x, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, tx_begin({s.at(2),
                                    transaction_options::transaction_type::LONG,
                                    {stz, stb}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(2), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(3), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CONFLICT_ON_WRITE_PRESERVE,
              search_key(s.at(3), stz, z, buf));
    ASSERT_EQ(Status::OK, tx_begin({s.at(4),
                                    transaction_options::transaction_type::LONG,
                                    {stb}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(4), sta, a, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(4), stb, b, v.at(4)));
    ASSERT_EQ(Status::OK, upsert(s.at(2), stz, z, v.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(2), stb, b, v.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, v.at(1)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), stb, b, v.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    // t2 wait t1 for boundary computation
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    // t4 wait t1's rx
    ASSERT_EQ(Status::OK, commit(s.at(4)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stb, b, buf));
    ASSERT_EQ(buf, v.at(4));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, commit(s.at(0)));

    // cleanup
    init_db();

    // test case 15
    // lllo
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {sty, stb}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), stx, x, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, tx_begin({s.at(2),
                                    transaction_options::transaction_type::LONG,
                                    {stz, stb}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(2), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, tx_begin({s.at(3),
                                    transaction_options::transaction_type::LONG,
                                    {sta, stb}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(3), stz, z, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(4), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CONFLICT_ON_WRITE_PRESERVE,
              search_key(s.at(4), sta, a, buf));
    ASSERT_EQ(Status::OK, upsert(s.at(3), sta, a, v.at(3)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), stb, b, v.at(3)));
    ASSERT_EQ(Status::OK, upsert(s.at(2), stz, z, v.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(2), stb, b, v.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, v.at(1)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), stb, b, v.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    // t2 wait t1 for boundary computation.
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    // t3 wait t2 for boundary computation.
    ASSERT_EQ(Status::OK, commit(s.at(3)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(3));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stb, b, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, commit(s.at(0)));

    // cleanup
    init_db();

    // test case 16
    // llll
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {sty, stb}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), stx, x, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, tx_begin({s.at(2),
                                    transaction_options::transaction_type::LONG,
                                    {stz, stb}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(2), sty, y, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, tx_begin({s.at(3),
                                    transaction_options::transaction_type::LONG,
                                    {sta, stb}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(3), stz, z, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, tx_begin({s.at(4),
                                    transaction_options::transaction_type::LONG,
                                    {stb}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(4), sta, a, buf));
    ASSERT_EQ(buf, v.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(4), stb, b, v.at(4)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), sta, a, v.at(3)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), stb, b, v.at(3)));
    ASSERT_EQ(Status::OK, upsert(s.at(2), stz, z, v.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(2), stb, b, v.at(2)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), sty, y, v.at(1)));
    ASSERT_EQ(Status::OK, upsert(s.at(1), stb, b, v.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    // t2 waits t1 for boundary computation.
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    // t3 waits t2 for boundary computation.
    ASSERT_EQ(Status::OK, commit(s.at(3)));
    // t4 waits t3 for boundary computation.
    ASSERT_EQ(Status::OK, commit(s.at(4)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, v.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, v.at(3));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stb, b, buf));
    ASSERT_EQ(buf, v.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, v.at(0));
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