
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

class sequent_two_readonly_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-anomaly-"
                                  "read_crown-sequent_two_readonly_test");
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

TEST_F(sequent_two_readonly_test, all) { // NOLINT
                                         // create table
    // ==========
    // prepare
    Storage stu{};
    Storage stv{};
    Storage stx{};
    Storage sty{};
    ASSERT_EQ(create_storage("u", stu), Status::OK);
    ASSERT_EQ(create_storage("v", stv), Status::OK);
    ASSERT_EQ(create_storage("x", stx), Status::OK);
    ASSERT_EQ(create_storage("y", sty), Status::OK);

    // enter
    std::array<Token, 6> s{};
    for (auto&& elem : s) { ASSERT_EQ(enter(elem), Status::OK); }

    // prepare page key value
    std::string u{"u"};
    std::string v{"v"};
    std::string x{"x"};
    std::string y{"y"};
    std::array<std::string, 6> var{"t0", "t1", "t2", "t3", "t4", "t5"};
    auto init_db = [&s, &var, u, v, x, y, stu, stv, stx, sty]() {
        epoch::set_perm_to_proc(epoch::ptp_init_val);
        ASSERT_EQ(Status::OK, upsert(s.at(0), stu, u, var.at(0)));
        ASSERT_EQ(Status::OK, upsert(s.at(0), stv, v, var.at(0)));
        ASSERT_EQ(Status::OK, upsert(s.at(0), stx, x, var.at(0)));
        ASSERT_EQ(Status::OK, upsert(s.at(0), sty, y, var.at(0)));
        ASSERT_EQ(Status::OK, commit(s.at(0)));
    };
    init_db();
    std::string buf{};
    // ==========

    // ==========
    // note: o is occ, l is ltx, x is omit
    // ltx4_prep_omit
    // lllxl
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {stx}}));
    ASSERT_EQ(Status::OK, tx_begin({s.at(2),
                                    transaction_options::transaction_type::LONG,
                                    {stu}}));
    ASSERT_EQ(Status::OK, tx_begin({s.at(3),
                                    transaction_options::transaction_type::LONG,
                                    {sty}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), stv, v, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(2), stv, v, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(1), stx, x, var.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(Status::OK, search_key(s.at(3), stx, x, buf));
    ASSERT_EQ(buf, var.at(0));
    //ASSERT_EQ(Status::OK,
    //          tx_begin({s.at(4), transaction_options::transaction_type::LONG}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, upsert(s.at(2), stu, u, var.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    //ASSERT_EQ(Status::OK, search_key(s.at(4), stx, x, buf));
    //ASSERT_EQ(buf, var.at(1));
    //ASSERT_EQ(Status::OK, search_key(s.at(4), stu, u, buf));
    //ASSERT_EQ(buf, var.at(2));
    //ASSERT_EQ(Status::OK, commit(s.at(4)));
    ASSERT_EQ(Status::OK,
              tx_begin({s.at(5), transaction_options::transaction_type::LONG}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(5), stu, u, buf));
    ASSERT_EQ(buf, var.at(2));
    ASSERT_EQ(Status::ERR_FAIL_WP, search_key(s.at(5), sty, y, buf));
    //ASSERT_EQ(Status::OK, commit(s.at(5)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), sty, y, var.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));

    // verify
    ASSERT_EQ(Status::OK, search_key(s.at(0), stu, u, buf));
    ASSERT_EQ(buf, var.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stv, v, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, var.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, var.at(3));
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