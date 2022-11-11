
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
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/record.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "index/yakushima/include/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_delete_insert_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "delete_insert-long_delete_insert_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google; // NOLINT
};

// start: one tx

TEST_F(long_delete_insert_test, same_tx_delete_insert) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // start data preparation
    ASSERT_EQ(insert(s, st, "", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s));
    // end data preparation

    // start test preparation
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    // end test preparation

    // test
    ASSERT_EQ(delete_record(s, st, ""), Status::OK);
    ASSERT_EQ(insert(s, st, "", "a"), Status::OK);
    // delete insert is update.
    ASSERT_EQ(Status::OK, commit(s));

    // verify
    std::string buf{};
    ASSERT_EQ(Status::OK, search_key(s, st, "", buf));
    ASSERT_EQ(buf, "a");

    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_delete_insert_test, same_tx_insert_delete) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // start test preparation
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    // end test preparation

    // test
    ASSERT_EQ(insert(s, st, "", ""), Status::OK);
    ASSERT_EQ(delete_record(s, st, ""), Status::WARN_CANCEL_PREVIOUS_INSERT);
    // delete insert is update.
    ASSERT_EQ(Status::OK, commit(s));

    // verify not found
    for (;;) {
        Record* rec_ptr{};
        auto rc = get<Record>(st, "", rec_ptr);
        if (rc == Status::WARN_NOT_FOUND) { break; }
        _mm_pause();
    }

    ASSERT_EQ(Status::OK, leave(s));
}

// end: one tx

// start: concurrent two tx

TEST_F(long_delete_insert_test, concurrent_insert_tx_delete_tx) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));

    // data preparation
    ASSERT_EQ(insert(s1, st, "", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s1));
    // end preparation

    // test preparation
    ASSERT_EQ(
            Status::OK,
            tx_begin({s1, transaction_options::transaction_type::LONG, {st}}));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    // preparation

    // test
    /**
     * s1 failed insert and depends on existing the records.
     * Internally, s1 executed tx read operation for the records.
     */
    ASSERT_EQ(Status::WARN_ALREADY_EXISTS, insert(s1, st, "", ""));
    std::string buf{};
    // s2 forward for s1
    ASSERT_EQ(delete_record(s2, st, ""), Status::OK);

    ASSERT_EQ(Status::OK, commit(s1));
    /**
     * s1 is treated that it executed read operation.
     * Forwarded s2's delete_record will break s1, so it fails validation.
     */
    ASSERT_EQ(Status::ERR_VALIDATION, commit(s2));

    ASSERT_EQ(Status::OK, leave(s2));
    ASSERT_EQ(Status::OK, leave(s1));
}

TEST_F(long_delete_insert_test, concurrent_delete_tx_insert_tx) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));

    // test preparation
    ASSERT_EQ(
            Status::OK,
            tx_begin({s1, transaction_options::transaction_type::LONG, {st}}));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    // preparation

    // test
    ASSERT_EQ(delete_record(s1, st, ""), Status::WARN_NOT_FOUND);
    // executed key read operation
    ASSERT_EQ(Status::OK, insert(s2, st, "", ""));
    // s2 forward for s1
    std::string buf{};

    ASSERT_EQ(Status::OK, commit(s1));
    /**
     * s1 is treated that it executed read operation.
     * Forwarded s2's insert will break s1, so it fails validation.
     */
    ASSERT_EQ(Status::ERR_VALIDATION, commit(s2));

    ASSERT_EQ(Status::OK, leave(s2));
    ASSERT_EQ(Status::OK, leave(s1));
}

// end: concurrent two tx

} // namespace shirakami::testing