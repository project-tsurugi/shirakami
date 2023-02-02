
#include <bitset>
#include <mutex>
#include <thread>

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class long_key_test : public ::testing::Test { // NOLINT

public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-common-"
                                  "scan-short_long_key_test");
        FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(long_key_test, long_key_search) { // NOLINT
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    for (std::size_t i = 1; i < 300; i++) {
        std::string k(i, 'A'); // search_key doesn't find with 256
        std::string v("a");    // NOLINT
        ASSERT_EQ(Status::OK, upsert(s, st, k, v));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT

        std::string sb{};
        ASSERT_EQ(Status::OK, search_key(s, st, k, sb));
        ASSERT_EQ(v, sb);
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    }

    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_key_test, 35kb_key_search) { // NOLINT
                                         // prepare
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::string sb{};
    // test
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              search_key(s, st, std::string(1024 * 35, 'a'), sb));

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_key_test, 36kb_key_search) { // NOLINT
                                         // prepare
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::string sb{};
    // test
    ASSERT_EQ(Status::WARN_INVALID_KEY_LENGTH,
              search_key(s, st, std::string(1024 * 36, 'a'), sb));

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing