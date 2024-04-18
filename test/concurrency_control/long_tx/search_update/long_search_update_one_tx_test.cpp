
#include <mutex>
#include <string>

#include "atomic_wrapper.h"
#include "test_tool.h"
#include "shirakami/interface.h"
#include "gtest/gtest.h"
#include "glog/logging.h"
#include "shirakami/api_storage.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"
#include "shirakami/transaction_options.h"

namespace shirakami::testing {

using namespace shirakami;

class long_search_update_one_tx_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-long_tx-"
                "search_update-long_search_update_one_tx_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

// start: one tx
TEST_F(long_search_update_one_tx_test, same_tx_update_search) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(enter(s), Status::OK);

    // prepare data
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s));
    wait_epoch_update();

    // test
    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(update(s, st, "", "1"), Status::OK);
    ASSERT_EQ(search_key(s, st, "", vb), Status::OK);
    ASSERT_EQ(vb, "1");
    ASSERT_EQ(Status::OK, commit(s));

    // clean up test
    ASSERT_EQ(leave(s), Status::OK);
}

TEST_F(long_search_update_one_tx_test, same_tx_search_update) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    ASSERT_EQ(enter(s), Status::OK);

    // prepare data
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s));
    wait_epoch_update();

    // test
    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(search_key(s, st, "", vb), Status::OK);
    ASSERT_EQ(vb, "");
    ASSERT_EQ(update(s, st, "", "1"), Status::OK);
    ASSERT_EQ(Status::OK, commit(s));

    // verify
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(search_key(s, st, "", vb), Status::OK);
    ASSERT_EQ(vb, "1");

    // clean up test
    ASSERT_EQ(leave(s), Status::OK);
}

// end: one tx

} // namespace shirakami::testing