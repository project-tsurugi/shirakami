
#include <array>
#include <mutex>

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class short_search_upsert : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-short_tx-"
                                  "search_upsert-short_search_upsert_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(short_search_upsert, simple) { // NOLINT
    Token s{};
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    ASSERT_EQ(enter(s), Status::OK);
    std::string k{"k"};
    std::string v{"v"};
    std::string vb{};
    ASSERT_EQ(search_key(s, st, k, vb), Status::WARN_NOT_FOUND);
    ASSERT_EQ(upsert(s, st, k, v), Status::OK);
    ASSERT_EQ(search_key(s, st, k, vb), Status::OK);
    ASSERT_EQ(memcmp(vb.data(), v.data(), v.size()), 0);
    ASSERT_EQ(commit(s), Status::OK);
    ASSERT_EQ(search_key(s, st, k, vb), Status::OK);
    ASSERT_EQ(memcmp(vb.data(), v.data(), v.size()), 0);
    ASSERT_EQ(commit(s), Status::OK);
    ASSERT_EQ(leave(s), Status::OK);
}

TEST_F(short_search_upsert, search_concurrent_upsert) { // NOLINT
    Storage st{};
    register_storage(st);
    std::string k("k"); // NOLINT
    std::string v("k"); // NOLINT
    std::array<Token, 2> token_ar{};
    ASSERT_EQ(Status::OK, enter(token_ar.at(0)));
    ASSERT_EQ(Status::OK, enter(token_ar.at(1)));
    ASSERT_EQ(Status::OK, upsert(token_ar.at(0), st, k, v));
    std::string vb{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(token_ar.at(1), st, k, vb));
    ASSERT_EQ(Status::OK, commit(token_ar.at(0))); // NOLINT
    ASSERT_EQ(Status::OK, commit(token_ar.at(1))); // NOLINT
    ASSERT_EQ(Status::OK, leave(token_ar.at(0)));
    ASSERT_EQ(Status::OK, leave(token_ar.at(1)));
}

} // namespace shirakami::testing