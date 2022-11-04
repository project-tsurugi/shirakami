
#include <array>
#include <bitset>
#include <mutex>
#include <thread>

#include "compiler.h"
#include "memory.h"

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"


namespace shirakami::testing {

using namespace shirakami;

Storage st{};
class simple_upsert : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "upsert-simple_upsert_test");
        FLAGS_stderrthreshold = 0; // output more than INFO
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

TEST_F(simple_upsert, read_only_mode_upsert) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    ASSERT_EQ(Status::WARN_ILLEGAL_OPERATION, upsert(s, {}, "", ""));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_upsert, upsert_at_non_existing_storage) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::WARN_STORAGE_NOT_FOUND, upsert(s, 5, "", ""));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_upsert, upsert) { // NOLINT
    std::string k("aaa");       // NOLINT
    std::string v("aaa");       // NOLINT
    std::string v2("bbb");      // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::WARN_ALREADY_EXISTS, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, upsert(s, st, k, v2));
    ASSERT_EQ(Status::OK, commit(s));
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s, st, k, vb));
    ASSERT_EQ(memcmp(vb.data(), v2.data(), v2.size()), 0);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing