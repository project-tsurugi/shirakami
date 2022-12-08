
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
#include "concurrency_control/include/wp.h"
#include "concurrency_control/include/wp_meta.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class simple_long_upsert_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "upsert-simple_long_upsert_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(simple_long_upsert_test, start_before_epoch) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    {
        std::unique_lock stop_epoch{epoch::get_ep_mtx()}; // stop epoch
        ASSERT_EQ(Status::OK,
                  tx_begin({s,
                            transaction_options::transaction_type::LONG,
                            {st}}));
        ASSERT_EQ(Status::WARN_PREMATURE, upsert(s, st, "", ""));
    } // start epoch
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_long_upsert_test, simple_long) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::string k{"k"};
    std::string v{"v"};
    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);

    wait_epoch_update();
    ASSERT_EQ(upsert(s, st, k, v), Status::OK);

    // check internal record existing
    auto check_internal_record_exist = [k](Storage st) {
        Record* rec{};
        ASSERT_EQ(Status::OK, get<Record>(st, k, rec));
        ASSERT_NE(rec, nullptr);
    };


    check_internal_record_exist(st);
    ASSERT_EQ(abort(s), Status::OK);
    // after abort, exist with deleted state.
    check_internal_record_exist(st);

    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    wait_epoch_update();
    ASSERT_EQ(upsert(s, st, k, v), Status::OK);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_long_upsert_test, insert_two_key_and_check_wp_result) { // NOLINT
                                                                      // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, insert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::LONG}));
    wait_epoch_update();
    std::string vb{};
    // block gc about write presreve result info
    ASSERT_EQ(Status::OK, search_key(s2, st, "", vb));
    ASSERT_EQ(Status::OK, upsert(s, st, "1", ""));
    ASSERT_EQ(Status::OK, upsert(s, st, "2", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // check
    wp::wp_meta* wp_meta_ptr{};
    ASSERT_EQ(Status::OK, wp::find_wp_meta(st, wp_meta_ptr));
    {
        std::shared_lock<std::shared_mutex> lk{
                wp_meta_ptr->get_mtx_wp_result_set()};
        wp::wp_meta::wp_result_elem_type elem =
                wp_meta_ptr->get_wp_result_set().front();
        std::tuple<bool, std::string, std::string>& target = std::get<3>(elem);
        ASSERT_EQ(std::get<0>(target), true);
        ASSERT_EQ(std::get<1>(target), "1");
        ASSERT_EQ(std::get<2>(target), "2");
    }

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing