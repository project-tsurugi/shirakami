
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
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "index/yakushima/include/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_delete_search_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "delete_search-long_delete_search_test");
        // FLAGS_stderrthreshold = 0;
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

TEST_F(long_delete_search_test, same_tx_delete_search) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // start data preparation
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
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
    std::string buf{};
    ASSERT_EQ(search_key(s, st, "", buf), Status::WARN_NOT_FOUND);
    ASSERT_EQ(Status::OK, commit(s));

    // verify
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_NE(Status::OK, search_key(s, st, "", buf));

    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_delete_search_test, same_tx_search_delete) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // start data preparation
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(insert(s, st, "", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s));
    // end data preparation

    // start test preparation
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    // end test preparation

    // test
    std::string buf{};
    ASSERT_EQ(search_key(s, st, "", buf), Status::OK);
    ASSERT_EQ(buf, "");
    ASSERT_EQ(delete_record(s, st, ""), Status::OK);
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

TEST_F(long_delete_search_test, concurrent_search_tx_delete_tx) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));

    // data preparation
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(insert(s1, st, "", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s1));
    // end preparation

    // test preparation
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::LONG}));
    wait_epoch_update();
    ASSERT_EQ(
            Status::OK,
            tx_begin({s2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    // preparation

    // test
    std::string buf{};
    ASSERT_EQ(Status::OK, search_key(s1, st, "", buf));
    ASSERT_EQ(buf, "");
    // s2 forward for s1
    ASSERT_EQ(delete_record(s2, st, ""), Status::OK);

    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(Status::OK, commit(s2));

    ASSERT_EQ(Status::OK, leave(s2));
    ASSERT_EQ(Status::OK, leave(s1));
}

TEST_F(long_delete_search_test, concurrent_delete_tx_search_tx) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));

    // data preparation
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(insert(s1, st, "", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s1));
    // end preparation

    // test preparation
    ASSERT_EQ(
            Status::OK,
            tx_begin({s1, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::LONG}));
    wait_epoch_update();
    // preparation

    // test
    ASSERT_EQ(delete_record(s1, st, ""), Status::OK);
    std::string buf{};
    ASSERT_EQ(Status::OK, search_key(s2, st, "", buf));
    ASSERT_EQ(buf, "");
    // s2 forward for s1

    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(Status::OK, commit(s2));

    ASSERT_EQ(Status::OK, leave(s2));
    ASSERT_EQ(Status::OK, leave(s1));
}

// end: concurrent two tx

} // namespace shirakami::testing
