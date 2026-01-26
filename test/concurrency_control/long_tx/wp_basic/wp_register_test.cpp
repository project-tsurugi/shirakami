
#include <xmmintrin.h>

#include <mutex>
#include <thread>

#include "concurrency_control/include/wp.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "test_tool.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::testing {

class wp_register_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-wp_"
                                  "basic-wp_register_test");
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

TEST_F(wp_register_test, single_register) { // NOLINT
    wp::wp_meta meta{};
    ASSERT_EQ(Status::OK, meta.register_wp(1, 1));
    auto rv = meta.get_wped();
    ASSERT_EQ(rv.at(0).first, 1);
    ASSERT_EQ(rv.at(0).second, 1);
    auto& wu{meta.get_wped_used()};
    ASSERT_EQ(wu.test(0), true);
    ASSERT_EQ(wu.count(), 1);
}

TEST_F(wp_register_test, multi_register) { // NOLINT
    wp::wp_meta meta{};
    ASSERT_EQ(Status::OK, meta.register_wp(1, 1));
    ASSERT_EQ(Status::OK, meta.register_wp(2, 2));
    auto rv = meta.get_wped();
    ASSERT_EQ(rv.at(0).first, 1);
    ASSERT_EQ(rv.at(0).second, 1);
    ASSERT_EQ(rv.at(1).first, 2);
    ASSERT_EQ(rv.at(1).second, 2);
    auto& wu{meta.get_wped_used()};
    ASSERT_EQ(wu.test(0), true);
    ASSERT_EQ(wu.test(1), true);
    ASSERT_EQ(wu.test(2), false);
    ASSERT_EQ(wu.count(), 2);
}

TEST_F(wp_register_test, shrink_at_commit) { // NOLINT
    /**
     * コミット時の write preserve 圧縮をテストする。
    */
    // prepare
    Storage st1{};
    Storage st2{};
    ASSERT_EQ(create_storage("test1", st1), Status::OK);
    ASSERT_EQ(create_storage("test2", st2), Status::OK);

    Token s{};
    ASSERT_EQ(enter(s), Status::OK);

    // test
    ASSERT_EQ(tx_begin({s,
                        transaction_options::transaction_type::LONG,
                        {st1, st2}}),
              Status::OK);
    wait_epoch_update();
    //  書くのは一か所のみ
    ASSERT_EQ(Status::OK, upsert(s, st1, "", ""));
    ASSERT_EQ(Status::OK, commit(s));

    // verify
    wp::wp_meta* wp_meta_ptr{};
    wp::find_wp_meta(st1, wp_meta_ptr);
    ASSERT_EQ(1, wp_meta_ptr->get_wp_result_set().size());
    wp::find_wp_meta(st2, wp_meta_ptr);
    ASSERT_EQ(0, wp_meta_ptr->get_wp_result_set().size());

    // cleanup
    ASSERT_EQ(leave(s), Status::OK);
}
} // namespace shirakami::testing
