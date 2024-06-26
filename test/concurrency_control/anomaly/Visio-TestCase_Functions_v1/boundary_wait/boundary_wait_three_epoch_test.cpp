
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

class boundary_wait_three_epoch_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-function-"
                "boundary_wait-boundary_wait_three_epoch_test");
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

TEST_F(boundary_wait_three_epoch_test, three_epoch) { // NOLINT
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
    std::array<std::string, 5> var{"t0", "t1", "t2", "t3", "t4"};
    auto init_db = [&s, &var, a, b, x, y, z, sta, stb, stx, sty, stz]() {
        epoch::set_perm_to_proc(epoch::ptp_init_val);
        ASSERT_EQ(Status::OK,
                  tx_begin({s.at(0),
                            transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, upsert(s.at(0), sta, a, var.at(0)));
        ASSERT_EQ(Status::OK, upsert(s.at(0), stb, b, var.at(0)));
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
    // 4l
    stop_epoch();
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {stb}}));
    ASSERT_EQ(Status::OK, tx_begin({s.at(2),
                                    transaction_options::transaction_type::LONG,
                                    {sta}}));
    ASSERT_EQ(Status::OK, tx_begin({s.at(3),
                                    transaction_options::transaction_type::LONG,
                                    {sty}}));
    ASSERT_EQ(Status::OK, tx_begin({s.at(4),
                                    transaction_options::transaction_type::LONG,
                                    {stz}}));
    resume_epoch();
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), stz, z, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(2), stx, x, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(3), stx, x, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(4), sty, y, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(4), stz, z, var.at(4)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), sty, y, var.at(3)));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(3), sta, a, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, var.at(2)));
    ASSERT_EQ(Status::OK, search_key(s.at(3), stb, b, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(1), stb, b, var.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(Status::OK, commit(s.at(3)));
    ASSERT_EQ(Status::ERR_CC, commit(s.at(4)));
    auto tri = transaction_result_info(s.at(4));
    ASSERT_EQ((*tri).get_reason_code(),
              reason_code::CC_LTX_WRITE_COMMITTED_READ_PROTECTION);

    // verify
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(0), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, var.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stb, b, buf));
    ASSERT_EQ(buf, var.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, var.at(3));
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

TEST_F(boundary_wait_three_epoch_test, three_epoch_with_abort) { // NOLINT
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
    std::array<std::string, 5> var{"t0", "t1", "t2", "t3", "t4"};
    auto init_db = [&s, &var, a, b, x, y, z, sta, stb, stx, sty, stz]() {
        epoch::set_perm_to_proc(epoch::ptp_init_val);
        ASSERT_EQ(Status::OK,
                  tx_begin({s.at(0),
                            transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, upsert(s.at(0), sta, a, var.at(0)));
        ASSERT_EQ(Status::OK, upsert(s.at(0), stb, b, var.at(0)));
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
    // 4l
    stop_epoch();
    ASSERT_EQ(Status::OK, tx_begin({s.at(1),
                                    transaction_options::transaction_type::LONG,
                                    {stb}}));
    ASSERT_EQ(Status::OK, tx_begin({s.at(2),
                                    transaction_options::transaction_type::LONG,
                                    {sta}}));
    ASSERT_EQ(Status::OK, tx_begin({s.at(3),
                                    transaction_options::transaction_type::LONG,
                                    {sty}}));
    ASSERT_EQ(Status::OK, tx_begin({s.at(4),
                                    transaction_options::transaction_type::LONG,
                                    {stz}}));
    resume_epoch();
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(1), stz, z, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(2), stx, x, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(3), stx, x, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(4), sty, y, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(4), stz, z, var.at(4)));
    ASSERT_EQ(Status::OK, upsert(s.at(3), sty, y, var.at(3)));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, search_key(s.at(3), sta, a, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(2), sta, a, var.at(2)));
    ASSERT_EQ(Status::OK, search_key(s.at(3), stb, b, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, upsert(s.at(1), stb, b, var.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(1)));
    ASSERT_EQ(Status::OK, commit(s.at(2)));
    ASSERT_EQ(Status::OK, abort(s.at(3)));
    ASSERT_EQ(Status::OK, commit(s.at(4)));

    // verify
    ASSERT_EQ(
            Status::OK,
            tx_begin({s.at(0), transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sta, a, buf));
    ASSERT_EQ(buf, var.at(2));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stb, b, buf));
    ASSERT_EQ(buf, var.at(1));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stx, x, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), sty, y, buf));
    ASSERT_EQ(buf, var.at(0));
    ASSERT_EQ(Status::OK, search_key(s.at(0), stz, z, buf));
    ASSERT_EQ(buf, var.at(4));
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
