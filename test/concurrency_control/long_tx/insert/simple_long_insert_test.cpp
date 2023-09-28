
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

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_insert_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "insert-simple_long_insert_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(long_insert_test, start_before_epoch) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    stop_epoch();
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG, {st}}));

    // test
    ASSERT_EQ(Status::WARN_PREMATURE, insert(s, st, "", ""));

    // cleanup
    resume_epoch();
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_insert_test, start_after_epoch) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();

    // test
    ASSERT_EQ(Status::OK, insert(s, st, "", "test"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // verify
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    std::string buf{};
    ASSERT_EQ(Status::OK, search_key(s, st, "", buf));
    ASSERT_EQ(buf, "test");

    // cleanup
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_insert_test, insert_two_key_and_check_wp_result) { // NOLINT
                                                               // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
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
    ASSERT_EQ(Status::OK, insert(s, st, "1", ""));
    ASSERT_EQ(Status::OK, insert(s, st, "2", ""));
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