#include <string.h>
#include <mutex>
#include <string>

#include "compiler.h"
#include "shirakami/interface.h"
#include "gtest/gtest.h"
#include "glog/logging.h"
#include "shirakami/api_storage.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"
#include "shirakami/transaction_options.h"

namespace shirakami::testing {

using namespace shirakami;

Storage st{};

class upsert_after_delete : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-short_tx-"
                "delete_upsert-short_upsert_after_delete_test");
        // FLAGS_stderrthreshold = 0;
    }
    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(upsert_after_delete, diff_tx) { // NOLINT
    create_storage("", st);
    std::string k("k"); // NOLINT
    std::string v("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, delete_record(s, st, k));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(upsert_after_delete, same_tx) { // NOLINT
    create_storage("", st);
    std::string k("k");   // NOLINT
    std::string v("v");   // NOLINT
    std::string v2("v2"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, delete_record(s, st, k));
    ASSERT_EQ(Status::OK, upsert(s, st, k, v2));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s, st, k, vb));
    ASSERT_EQ(memcmp(vb.data(), v2.data(), v2.size()), 0);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
}

} // namespace shirakami::testing
