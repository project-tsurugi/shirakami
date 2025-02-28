
#include <xmmintrin.h>

#include <mutex>
#include <thread>

#include "test_tool.h"

#include "concurrency_control/include/read_plan.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/wp.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::testing {

class read_area_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-wp_"
                                  "basic-read_area_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google; // NOLINT
};

TEST_F(read_area_test, register_same_st) { // NOLINT
    Token s{};
    Storage st{};
    Storage st2{};
    ASSERT_EQ(Status::OK, create_storage("1", st));
    ASSERT_EQ(Status::OK, create_storage("2", st2));
    ASSERT_EQ(Status::OK, enter(s));
    // register positive twice
    ASSERT_EQ(Status::OK, tx_begin({s,
                                    transaction_options::transaction_type::LONG,
                                    {},
                                    {{st, st}, {st2, st2}}}));

    // check global table
    {
        std::shared_lock<std::shared_mutex> lk{read_plan::get_mtx_cont()};
        ASSERT_EQ(read_plan::get_cont().size(), 1);
        auto ra = *read_plan::get_cont().begin();
        ASSERT_EQ(std::get<0>(ra.second).size(), 1);
        ASSERT_EQ(std::get<1>(ra.second).size(), 1);
    }

    // check local worker info
    auto* ti = static_cast<session*>(s);
    // check positive
    {
        auto set = ti->get_read_area().get_positive_list(); // NOLINT
        ASSERT_EQ(1, set.size());
        ASSERT_EQ(st, *set.begin());
    }
    // check negative
    {
        auto set = ti->get_read_area().get_negative_list(); // NOLINT
        ASSERT_EQ(1, set.size());
        ASSERT_EQ(st2, *set.begin());
    }

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(read_area_test, register_and_remove_posi_only_commit) { // NOLINT
    Token s{};
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin({s,
                                    transaction_options::transaction_type::LONG,
                                    {},
                                    {{st}, {}}}));
    // check global table
    {
        std::shared_lock<std::shared_mutex> lk{read_plan::get_mtx_cont()};
        ASSERT_EQ(read_plan::get_cont().size(), 1);
        auto ra = *read_plan::get_cont().begin();
        ASSERT_EQ(std::get<0>(ra.second).size(), 1);
        ASSERT_EQ(std::get<1>(ra.second).size(), 0);
    }

    // commit erase above info
    wait_epoch_update();
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // check no existence about read area
    // check global table
    {
        std::shared_lock<std::shared_mutex> lk{read_plan::get_mtx_cont()};
        ASSERT_EQ(read_plan::get_cont().size(), 0);
    }


    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(read_area_test, register_and_remove_nega_only_commit) { // NOLINT
    Token s{};
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin({s,
                                    transaction_options::transaction_type::LONG,
                                    {},
                                    {{}, {st}}}));
    // check global table
    {
        std::shared_lock<std::shared_mutex> lk{read_plan::get_mtx_cont()};
        ASSERT_EQ(read_plan::get_cont().size(), 1);
        auto ra = *read_plan::get_cont().begin();
        ASSERT_EQ(std::get<0>(ra.second).size(), 0);
        ASSERT_EQ(std::get<1>(ra.second).size(), 1);
    }

    // commit erase above info
    wait_epoch_update();
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // check no existence about read area
    // check global table
    {
        std::shared_lock<std::shared_mutex> lk{read_plan::get_mtx_cont()};
        ASSERT_EQ(read_plan::get_cont().size(), 0);
    }

    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(read_area_test, register_and_remove_posi_only_abort) { // NOLINT
    Token s{};
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin({s,
                                    transaction_options::transaction_type::LONG,
                                    {},
                                    {{st}, {}}}));
    // check global table
    {
        std::shared_lock<std::shared_mutex> lk{read_plan::get_mtx_cont()};
        ASSERT_EQ(read_plan::get_cont().size(), 1);
        auto ra = *read_plan::get_cont().begin();
        ASSERT_EQ(std::get<0>(ra.second).size(), 1);
        ASSERT_EQ(std::get<1>(ra.second).size(), 0);
    }

    // commit erase above info
    wait_epoch_update();
    ASSERT_EQ(Status::OK, abort(s)); // NOLINT

    // check no existence about read area
    // check global table
    {
        std::shared_lock<std::shared_mutex> lk{read_plan::get_mtx_cont()};
        ASSERT_EQ(read_plan::get_cont().size(), 0);
    }


    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(read_area_test, register_and_remove_nega_only_abort) { // NOLINT
    Token s{};
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin({s,
                                    transaction_options::transaction_type::LONG,
                                    {},
                                    {{}, {st}}}));
    // check global table
    {
        std::shared_lock<std::shared_mutex> lk{read_plan::get_mtx_cont()};
        ASSERT_EQ(read_plan::get_cont().size(), 1);
        auto ra = *read_plan::get_cont().begin();
        ASSERT_EQ(std::get<0>(ra.second).size(), 0);
        ASSERT_EQ(std::get<1>(ra.second).size(), 1);
    }

    // commit erase above info
    wait_epoch_update();
    ASSERT_EQ(Status::OK, abort(s)); // NOLINT

    // check no existence about read area
    // check global table
    {
        std::shared_lock<std::shared_mutex> lk{read_plan::get_mtx_cont()};
        ASSERT_EQ(read_plan::get_cont().size(), 0);
    }

    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(read_area_test, conflict_positive_negative) { // NOLINT
                                                     // prepare
    Token s{};
    Storage st1{};
    Storage st2{};
    Storage st3{};
    ASSERT_EQ(Status::OK, create_storage("1", st1));
    ASSERT_EQ(Status::OK, create_storage("2", st2));
    ASSERT_EQ(Status::OK, create_storage("3", st3));
    ASSERT_EQ(Status::OK, enter(s));
    // positive 1, 2. negative 2, 3.
    // result: positive 1, negative 2, 3.
    ASSERT_EQ(Status::OK, tx_begin({s,
                                    transaction_options::transaction_type::LONG,
                                    {},
                                    {{st1, st2}, {st2, st3}}}));

    // check global table
    {
        std::shared_lock<std::shared_mutex> lk{read_plan::get_mtx_cont()};
        ASSERT_EQ(read_plan::get_cont().size(), 1);
        auto ra = *read_plan::get_cont().begin();
        ASSERT_EQ(std::get<0>(ra.second).size(), 1);
        ASSERT_EQ(std::get<1>(ra.second).size(), 2);
    }

    // check local worker info
    auto* ti = static_cast<session*>(s);
    // check positive
    {
        auto set = ti->get_read_area().get_positive_list(); // NOLINT
        ASSERT_EQ(1, set.size());
        ASSERT_EQ(st1, *set.begin());
    }
    // check negative
    {
        auto set = ti->get_read_area().get_negative_list(); // NOLINT
        ASSERT_EQ(2, set.size());
        auto itr = set.begin();
        ASSERT_EQ(st2, *itr);
        ++itr;
        ASSERT_EQ(st3, *(itr));
    }

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(read_area_test, ut_check_range_overlap) {
    // note(category): define write range [], read range (), write end is read end {}
    // note(set): open end (), closed end []

    // same {}
    // w:["1":"1"] vs r:["1":"1"] -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "1", "1", scan_endpoint::INCLUSIVE, "1", scan_endpoint::INCLUSIVE));
    // w:["1":"5"] vs r:["1":"5"] -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "5", "1", scan_endpoint::INCLUSIVE, "5", scan_endpoint::INCLUSIVE));

    // range overlap [(]), write is left
    // w:["1":"3"] vs r:["2":"4"] -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "2", scan_endpoint::INCLUSIVE, "4", scan_endpoint::INCLUSIVE));
    // w:["1":"3"] vs r:("2":"4") -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "2", scan_endpoint::EXCLUSIVE, "4", scan_endpoint::EXCLUSIVE));
    // w:["1":"3"] vs r:("1":"4") -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "1", scan_endpoint::EXCLUSIVE, "4", scan_endpoint::EXCLUSIVE));
    // w:["1":"3"] vs r:["2":inf) -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "2", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF));
    // w:["1":"3"] vs r:("2":inf) -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "2", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF));
    // w:["1":"3"] vs r:("1":inf) -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "1", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF));
    // range overlap ([)], write is right
    // w:["1":"3"] vs r:["0":"2"] -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "0", scan_endpoint::INCLUSIVE, "2", scan_endpoint::INCLUSIVE));
    // w:["1":"3"] vs r:("0":"2") -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "0", scan_endpoint::EXCLUSIVE, "2", scan_endpoint::EXCLUSIVE));
    // w:["1":"3"] vs r:("0":"3") -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "0", scan_endpoint::EXCLUSIVE, "3", scan_endpoint::EXCLUSIVE));
    // w:["1":"3"] vs r:(-inf:"3") -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "", scan_endpoint::INF, "3", scan_endpoint::EXCLUSIVE));
    // w:["1":"3"] vs r:(-inf:"2"] -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "", scan_endpoint::INF, "2", scan_endpoint::INCLUSIVE));
    // w:["1":"3"] vs r:(-inf:"2") -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "", scan_endpoint::INF, "2", scan_endpoint::EXCLUSIVE));

    // containing ([]), write is smaller
    // w:["1":"3"] vs r:["0":"4"] -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "0", scan_endpoint::INCLUSIVE, "4", scan_endpoint::INCLUSIVE));
    // w:["1":"3"] vs r:("0":"4") -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "0", scan_endpoint::EXCLUSIVE, "4", scan_endpoint::EXCLUSIVE));
    // w:["1":"3"] vs r:["0":inf) -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "0", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF));
    // w:["1":"3"] vs r:("0":inf) -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "0", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF));
    // w:["1":"3"] vs r:(-inf:"4"] -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "", scan_endpoint::INF, "4", scan_endpoint::INCLUSIVE));
    // w:["1":"3"] vs r:(-inf:"4") -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "", scan_endpoint::INF, "4", scan_endpoint::EXCLUSIVE));
    // w:["1":"3"] vs r:(-inf:inf) -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "", scan_endpoint::INF, "", scan_endpoint::INF));
    // containing [()], write is larger
    // w:["0":"4"] vs r:["1":"3"] -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("0", "4", "1", scan_endpoint::INCLUSIVE, "3", scan_endpoint::INCLUSIVE));
    // w:["0":"4"] vs r:("1":"3") -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("0", "4", "1", scan_endpoint::EXCLUSIVE, "3", scan_endpoint::EXCLUSIVE));
    // w:["0":"4"] vs r:("0":"4") -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("0", "4", "0", scan_endpoint::EXCLUSIVE, "4", scan_endpoint::EXCLUSIVE));
    // containing {)], write is larger, left is same
    // w:["1":"3"] vs r:["1":"2"] -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "1", scan_endpoint::INCLUSIVE, "2", scan_endpoint::INCLUSIVE));
    // w:["1":"3"] vs r:["1":"2") -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "1", scan_endpoint::INCLUSIVE, "2", scan_endpoint::EXCLUSIVE));
    // w:["1":"3"] vs r:["1":"3") -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "1", scan_endpoint::INCLUSIVE, "3", scan_endpoint::EXCLUSIVE));
    // containing {]), write is smaller, left is same
    // w:["1":"3"] vs r:["1":"4"] -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "1", scan_endpoint::INCLUSIVE, "4", scan_endpoint::INCLUSIVE));
    // w:["1":"3"] vs r:["1":"4") -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "1", scan_endpoint::INCLUSIVE, "4", scan_endpoint::EXCLUSIVE));
    // w:["1":"3"] vs r:["1":inf) -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "1", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF));
    // containing [(}, write is larger, right is same
    // w:["1":"3"] vs r:["2":"3"] -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "2", scan_endpoint::INCLUSIVE, "3", scan_endpoint::INCLUSIVE));
    // w:["1":"3"] vs r:("2":"3"] -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "2", scan_endpoint::EXCLUSIVE, "3", scan_endpoint::INCLUSIVE));
    // w:["1":"3"] vs r:("1":"3"] -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "1", scan_endpoint::EXCLUSIVE, "3", scan_endpoint::INCLUSIVE));
    // containing ([}, write is smaller, right is same
    // w:["1":"3"] vs r:["0":"3"] -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "0", scan_endpoint::INCLUSIVE, "3", scan_endpoint::INCLUSIVE));
    // w:["1":"3"] vs r:(-inf:"3"] -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "", scan_endpoint::INF, "3", scan_endpoint::INCLUSIVE));

    // separated [](), write is left
    // w:["1":"2"] vs r:["4":"5"] -> miss
    EXPECT_FALSE(read_plan::check_range_overlap("1", "2", "4", scan_endpoint::INCLUSIVE, "5", scan_endpoint::INCLUSIVE));
    // w:["1":"2"] vs r:("4":"5") -> miss
    EXPECT_FALSE(read_plan::check_range_overlap("1", "2", "4", scan_endpoint::EXCLUSIVE, "5", scan_endpoint::EXCLUSIVE));
    // separated ()[], write is right
    // w:["4":"5"] vs r:["1":"2"] -> miss
    EXPECT_FALSE(read_plan::check_range_overlap("4", "5", "1", scan_endpoint::INCLUSIVE, "2", scan_endpoint::INCLUSIVE));
    // w:["4":"5"] vs r:("1":"2") -> miss
    EXPECT_FALSE(read_plan::check_range_overlap("4", "5", "1", scan_endpoint::EXCLUSIVE, "2", scan_endpoint::EXCLUSIVE));

    // touching [](), write is left
    // w:["1":"3"] vs r:["3":"5"] -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "3", scan_endpoint::INCLUSIVE, "5", scan_endpoint::INCLUSIVE));
    // w:["1":"3"] vs r:["3":inf) -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("1", "3", "3", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF));
    // w:["1":"3"] vs r:("3":"5") -> miss
    EXPECT_FALSE(read_plan::check_range_overlap("1", "3", "3", scan_endpoint::EXCLUSIVE, "5", scan_endpoint::EXCLUSIVE));
    // w:["1":"3"] vs r:("3":inf) -> miss
    EXPECT_FALSE(read_plan::check_range_overlap("1", "3", "3", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF));
    // touching ()[], write is right
    // w:["3":"5"] vs r:["1":"3"] -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("3", "5", "1", scan_endpoint::INCLUSIVE, "3", scan_endpoint::INCLUSIVE));
    // w:["3":"5"] vs r:(-inf:"3"] -> hit
    EXPECT_TRUE(read_plan::check_range_overlap("3", "5", "", scan_endpoint::INF, "3", scan_endpoint::INCLUSIVE));
    // w:["3":"5"] vs r:("1":"3") -> miss
    EXPECT_FALSE(read_plan::check_range_overlap("3", "5", "1", scan_endpoint::EXCLUSIVE, "3", scan_endpoint::EXCLUSIVE));
    // w:["3":"5"] vs r:(-inf:"3") -> miss
    EXPECT_FALSE(read_plan::check_range_overlap("3", "5", "", scan_endpoint::INF, "3", scan_endpoint::EXCLUSIVE));
}

} // namespace shirakami::testing
