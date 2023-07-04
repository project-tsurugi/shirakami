
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

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_delete_update_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "delete_update-long_delete_update_test");
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

// start: one tx

TEST_F(long_delete_update_test, same_tx_update_delete) { // NOLINT
                                                         // prepare
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();

    // test
    ASSERT_EQ(Status::OK, update(s, st, "", ""));
    ASSERT_EQ(Status::OK, delete_record(s, st, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // verify
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    std::string buf{};
    ASSERT_NE(Status::OK, search_key(s, st, "", buf));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_delete_update_test, same_tx_delete_update) { // NOLINT
                                                         // prepare
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();

    // test
    ASSERT_EQ(Status::OK, delete_record(s, st, ""));
    ASSERT_EQ(Status::WARN_NOT_FOUND, update(s, st, "", "a"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // verify
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    std::string buf{};
    ASSERT_NE(Status::OK, search_key(s, st, "", buf));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ASSERT_EQ(Status::OK, leave(s));
}

// end: one tx

// start: concurrent two tx

TEST_F(long_delete_update_test, concurrent_update_tx_delete_tx) { // NOLINT
                                                                  // prepare
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s1, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT
    ASSERT_EQ(
            Status::OK,
            tx_begin({s1, transaction_options::transaction_type::LONG, {st}}));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();

    // test
    ASSERT_EQ(Status::OK, update(s1, st, "", "a"));
    ASSERT_EQ(Status::OK,
              delete_record(s2, st, ""));  // forwarding to same epoch
    ASSERT_EQ(Status::OK, commit(s1));     // NOLINT
    ASSERT_EQ(Status::ERR_CC, commit(s2)); // NOLINT

    // verify
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    std::string buf{};
    ASSERT_EQ(Status::OK, search_key(s1, st, "", buf));
    ASSERT_EQ(buf, "a");
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(long_delete_update_test, concurrent_delete_tx_update_tx) { // NOLINT
                                                                  // prepare
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s1, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT
    ASSERT_EQ(
            Status::OK,
            tx_begin({s1, transaction_options::transaction_type::LONG, {st}}));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();

    // test
    ASSERT_EQ(Status::OK, delete_record(s1, st, ""));
    ASSERT_EQ(Status::OK, update(s2, st, "", "")); // forwarding
    ASSERT_EQ(Status::OK, commit(s1));             // NOLINT
    ASSERT_EQ(Status::ERR_CC, commit(s2));         // NOLINT
    /**
     * due to key-value combined read information.
     */

    // verify
    for (;;) {
        Record* rec_ptr{};
        auto rc = get<Record>(st, "", rec_ptr);
        if (rc == Status::WARN_NOT_FOUND) { break; }
        _mm_pause();
    }

    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

// end: concurrent two tx

} // namespace shirakami::testing