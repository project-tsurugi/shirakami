
#include <xmmintrin.h>

#include <mutex>
#include <thread>

#include "test_tool.h"

#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/wp.h"

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

TEST_F(read_area_test, register_and_remove_posi) { // NOLINT
    Token s{};
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin({s,
                                    transaction_options::transaction_type::LONG,
                                    {},
                                    {{st}, {}}}));
    // check existence about read area
    wp::page_set_meta* out{};
    ASSERT_EQ(Status::OK, find_page_set_meta(st, out));
    // check positive list
    read_plan::list_type list = out->get_read_plan().get_positive_list();
    ASSERT_EQ(list.size(), 1);
    auto* ti = static_cast<session*>(s);
    ASSERT_EQ(ti->get_long_tx_id(), *list.begin());
    // check negative list
    list = out->get_read_plan().get_negative_list();
    ASSERT_EQ(list.size(), 0);

    // commit erase above info
    wait_epoch_update();
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // check no existence about read area
    list = out->get_read_plan().get_positive_list();
    ASSERT_EQ(list.size(), 0);
    list = out->get_read_plan().get_negative_list();
    ASSERT_EQ(list.size(), 0);

    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(read_area_test, register_and_remove_nega) { // NOLINT
    Token s{};
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin({s,
                                    transaction_options::transaction_type::LONG,
                                    {},
                                    {{}, {st}}}));
    // check existence about read area
    wp::page_set_meta* out{};
    ASSERT_EQ(Status::OK, find_page_set_meta(st, out));
    // check negative list
    read_plan::list_type list = out->get_read_plan().get_negative_list();
    ASSERT_EQ(list.size(), 1);
    auto* ti = static_cast<session*>(s);
    ASSERT_EQ(ti->get_long_tx_id(), *list.begin());
    // check positive list
    list = out->get_read_plan().get_positive_list();
    ASSERT_EQ(list.size(), 0);

    // commit erase above info
    wait_epoch_update();
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // check no existence about read area
    list = out->get_read_plan().get_positive_list();
    ASSERT_EQ(list.size(), 0);
    list = out->get_read_plan().get_negative_list();
    ASSERT_EQ(list.size(), 0);

    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing