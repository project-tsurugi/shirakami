
#include <bitset>
#include <mutex>
#include <thread>

#include "concurrency_control/include/record.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class short_upsert_long_key : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-short_tx-"
                                  "upsert-short_upsert_long_key_test");
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

TEST_F(short_upsert_long_key, long_key_upsert) { // NOLINT
#ifdef __clang__
    GTEST_SKIP() << "FIXME: clang Debug build may exceed max stack size";
#endif
    Storage st{};
    create_storage("", st);
    std::string k(1024 * 30, '0'); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, k, "v"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(short_upsert_long_key, over_30kb_key_upsert) { // NOLINT
    Storage st{};
    create_storage("", st);
    std::string k(1024 * 36, '0'); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::WARN_INVALID_KEY_LENGTH, upsert(s, st, k, "v"));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
