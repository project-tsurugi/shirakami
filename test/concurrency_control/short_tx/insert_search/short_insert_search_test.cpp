
#include <mutex>
#include <array>
#include <string>

#include "gtest/gtest.h"
#include "shirakami/interface.h"
#include "glog/logging.h"
#include "shirakami/api_diagnostic.h"
#include "shirakami/api_storage.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"
#include "shirakami/transaction_options.h"

namespace shirakami::testing {

using namespace shirakami;

class short_insert_search_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-short_tx-"
                                  "insert_search-short_insert_search_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
    }
    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(short_insert_search_test,                                     // NOLINT
       search_can_find_concurrent_inserting_by_insert_commit_fail) { // NOLINT
    Storage st{};
    create_storage("", st);
    std::string k("k"); // NOLINT
    std::string v("k"); // NOLINT
    std::array<Token, 2> token_ar{};
    ASSERT_EQ(Status::OK, enter(token_ar.at(0)));
    ASSERT_EQ(Status::OK, enter(token_ar.at(1)));
    ASSERT_EQ(Status::OK,
              tx_begin({token_ar.at(0),
                        transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(token_ar.at(0), st, k, v));
    ASSERT_EQ(Status::OK,
              tx_begin({token_ar.at(1),
                        transaction_options::transaction_type::SHORT}));
    std::string vb{};
    ASSERT_EQ(Status::WARN_CONCURRENT_INSERT,
              search_key(token_ar.at(1), st, k, vb));
    ASSERT_EQ(Status::OK, commit(token_ar.at(0))); // NOLINT
    // verify inserted target and fail it
    ASSERT_EQ(Status::ERR_CC, commit(token_ar.at(1))); // NOLINT
    ASSERT_EQ(Status::OK, leave(token_ar.at(0)));
    ASSERT_EQ(Status::OK, leave(token_ar.at(1)));
}

TEST_F(short_insert_search_test, // NOLINT
       search_can_find_concurrent_inserting_by_insert_commit_success) { // NOLINT
    Storage st{};
    create_storage("", st);
    std::string k("k"); // NOLINT
    std::string v("k"); // NOLINT
    std::array<Token, 2> token_ar{};
    ASSERT_EQ(Status::OK, enter(token_ar.at(0)));
    ASSERT_EQ(Status::OK, enter(token_ar.at(1)));
    ASSERT_EQ(Status::OK,
              tx_begin({token_ar.at(0),
                        transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(token_ar.at(0), st, k, v));
    ASSERT_EQ(Status::OK,
              tx_begin({token_ar.at(1),
                        transaction_options::transaction_type::SHORT}));
    std::string vb{};
    ASSERT_EQ(Status::WARN_CONCURRENT_INSERT,
              search_key(token_ar.at(1), st, k, vb));
    // verify inserting target and success it
    ASSERT_EQ(Status::OK, commit(token_ar.at(1))); // NOLINT
    ASSERT_EQ(Status::OK, commit(token_ar.at(0))); // NOLINT
    ASSERT_EQ(Status::OK, leave(token_ar.at(0)));
    ASSERT_EQ(Status::OK, leave(token_ar.at(1)));
}

} // namespace shirakami::testing
