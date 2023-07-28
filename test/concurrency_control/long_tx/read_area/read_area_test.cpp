
#include <xmmintrin.h>

#include <mutex>
#include <thread>

#include "test_tool.h"

#include "concurrency_control/include/read_plan.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
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
        FLAGS_stderrthreshold = 0; // output more than INFO
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
        ASSERT_EQ(ra.second.get_positive_list().size(), 1);
        ASSERT_EQ(ra.second.get_negative_list().size(), 1);
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
        ASSERT_EQ(ra.second.get_positive_list().size(), 1);
        ASSERT_EQ(ra.second.get_negative_list().size(), 0);
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
        ASSERT_EQ(ra.second.get_positive_list().size(), 0);
        ASSERT_EQ(ra.second.get_negative_list().size(), 1);
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
        ASSERT_EQ(ra.second.get_positive_list().size(), 1);
        ASSERT_EQ(ra.second.get_negative_list().size(), 0);
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
        ASSERT_EQ(ra.second.get_positive_list().size(), 0);
        ASSERT_EQ(ra.second.get_negative_list().size(), 1);
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
        ASSERT_EQ(ra.second.get_positive_list().size(), 1);
        ASSERT_EQ(ra.second.get_negative_list().size(), 2);
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

} // namespace shirakami::testing