
#include <bitset>
#include <mutex>
#include <thread>

#include "atomic_wrapper.h"

#include "concurrency_control/include/record.h"
#include "concurrency_control/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class short_delete_insert_search_upsert_test
    : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-short_tx-"
                                  "delete_insert_search_upsert-short_delete_"
                                  "insert_search_upsert_test");
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

TEST_F(short_delete_insert_search_upsert_test, // NOLINT
       insert_after_delete_upsert) {           // NOLINT
    Storage st{};
    create_storage("", st);
    std::string k1("k1");
    std::string k2("k2");
    std::string v1("v1");
    std::string v2("v2");
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, st, k2, v2));
    ASSERT_EQ(Status::OK, commit(s));
    {
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, delete_record(s, st, k2));
        ASSERT_EQ(Status::OK, upsert(s, st, k1, v1));
        EXPECT_EQ(Status::WARN_ALREADY_EXISTS, insert(s, st, k1, v1));
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s, st, k1, vb));
    ASSERT_EQ(v1, vb);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
