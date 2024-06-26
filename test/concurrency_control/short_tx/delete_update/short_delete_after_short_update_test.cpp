
#include <mutex>
#include <memory>

#include "shirakami/interface.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "shirakami/api_storage.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"
#include "shirakami/transaction_options.h"

namespace shirakami::testing {

class short_delete_after_short_update : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-short_tx-delete_update-"
                "short_delete_after_short_update_test");
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

TEST_F(short_delete_after_short_update, delete_after_update) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s{};

    // prepare data
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s));

    // start test
    // same tx
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, update(s, st, "", ""));
    ASSERT_EQ(Status::OK, delete_record(s, st, ""));
    ASSERT_EQ(Status::OK, commit(s));

    // different tx
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::WARN_NOT_FOUND, update(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s));

    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
