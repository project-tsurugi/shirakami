
#include <glog/logging.h>
#include <mutex>
#include <string>

#include "gtest/gtest.h"
#include "shirakami/interface.h"
#include "shirakami/api_storage.h"
#include "shirakami/binary_printer.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"
#include "shirakami/transaction_options.h"

namespace shirakami::testing {

using namespace shirakami;

class short_t1_delete_between_t2_delete_upsert_test
    : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-short_tx-"
                "delete_short_t1_delete_between_t2_delete_upsert_test");
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

TEST_F(short_t1_delete_between_t2_delete_upsert_test, delete_upsert) { // NOLINT
    // prepare
    Storage st{};
    create_storage("", st);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s1, st, "", "1"));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    // test
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, delete_record(s1, st, ""));
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, delete_record(s2, st, ""));
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT
    ASSERT_EQ(Status::OK, upsert(s1, st, "", "2"));
    // checked local delete and change it to update.
    ASSERT_EQ(Status::ERR_KVS, commit(s1)); // NOLINT

    // verify
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    std::string buf{};
    ASSERT_NE(Status::OK, search_key(s1, st, "", buf));
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing
