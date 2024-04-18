
#include <mutex>
#include <cstddef>
#include <ostream>
#include <string>

#include "compiler.h"
#include "memory.h"
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
class upsert_after_upsert : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "upsert-upsert_after_upsert_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
    }
    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
        create_storage("", st);
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(upsert_after_upsert, double_upsert_same_tx) { // NOLINT
    // double upsert by the same tx
    std::string k("aaa");  // NOLINT
    std::string v("bbb");  // NOLINT
    std::string v2("ccc"); // NOLINT
    std::string v3("ddd"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, k, v2));
    ASSERT_EQ(Status::OK, upsert(s, st, k, v3));
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s, st, k, vb));
    ASSERT_EQ(vb, v3);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(upsert_after_upsert, double_upsert_diff_tx) { // NOLINT
    // double upsert by different tx

    // prepare
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s1, st, "", "1"));
    ASSERT_EQ(Status::OK, upsert(s2, st, "", "2"));
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(Status::OK, commit(s2));
    std::string buf{};
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s1, st, "", buf));
    ASSERT_EQ(buf, "2");
    ASSERT_EQ(Status::OK, commit(s1));

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(upsert_after_upsert, multi_upsert) { // NOLINT
    std::string k("K");
    std::string v(1000, 'v'); // NOLINT
    Token s{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(s2));

    ASSERT_EQ(tx_begin({s2}), Status::OK); // to block gc

    for (std::size_t i = 0; i < 100; ++i) { // NOLINT
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, upsert(s, st, k, v));
        ASSERT_EQ(Status::OK, commit(s));
    }

    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(s2));
    LOG(INFO) << "maxrss:\t" << getRusageRUMaxrss();
}

} // namespace shirakami::testing
