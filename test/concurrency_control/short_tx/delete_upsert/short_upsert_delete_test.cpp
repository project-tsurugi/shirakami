#include <mutex>
#include <string>
#include <string_view>

#include "compiler.h"
#include "shirakami/interface.h"
#include "gtest/gtest.h"
#include "glog/logging.h"
#include "shirakami/api_storage.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"
#include "shirakami/transaction_options.h"
#include "test_tool.h"

namespace shirakami::testing {

using namespace shirakami;

Storage st{};

class upsert_delete : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-short_tx-"
                "delete_upsert-short_upsert_delete_test_test");
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

TEST_F(upsert_delete, simple) { // NOLINT
    create_storage("", st);
    std::string k("k"); // NOLINT
    std::string v("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // prepare
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, k, "v"));
    ASSERT_EQ(Status::OK, commit(s));

    // test
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, k, "v2"));
    ASSERT_EQ(Status::OK, delete_record(s, st, k));
    ASSERT_EQ(Status::OK, commit(s));

    // verify
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    std::string buf{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, st, k, buf));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(upsert_delete, wso_any2absent) { // NOLINT
    // UPSERT + DELETE -> DELSERT
    create_storage("", st);
    std::string k("k"); // NOLINT
    Token s{};
    ASSERT_OK(enter(s));

    // prepare
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, k, "v"));
    ASSERT_OK(commit(s));

    // test
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, k, "v2"));
    ASSERT_OK(delete_record(s, st, k));
    ASSERT_OK(commit(s));

    // verify
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    std::string buf{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, st, k, buf));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_OK(leave(s));
}

TEST_F(upsert_delete, wso_any2absent_vs_insert) { // NOLINT
    // UPSERT + DELETE -> DELSERT
    create_storage("", st);
    std::string_view k("k");
    Token s1{};
    Token s2{};
    std::string buf{};
    ASSERT_OK(enter(s1));
    ASSERT_OK(enter(s2));

    // prepare

    // test
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(tx_begin({s2, transaction_options::transaction_type::SHORT}));

    ASSERT_OK(upsert(s1, st, k, "v1"));
    ASSERT_OK(insert(s2, st, k, "v2"));
    ASSERT_OK(delete_record(s1, st, k));  // ANY_TO_ABSENT (DELSERT)
    ASSERT_OK(commit(s2)); // "k" : "v2"
    ASSERT_OK(commit(s1)); // "k" : delete

    // verify
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s1, st, k, buf));
    ASSERT_OK(commit(s1));

    ASSERT_OK(leave(s2));
    ASSERT_OK(leave(s1));
}

} // namespace shirakami::testing
