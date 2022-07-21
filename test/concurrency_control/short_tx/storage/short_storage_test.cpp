
#include <mutex>

#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class storage : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-silo-storage-storage_test");
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

TEST_F(storage, storage_not_used_storage_key) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage(st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(storage, storage_used_storage_key) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("NAUTI", st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(storage, multiple_storages) { // NOLINT
    Storage storage0{};
    Storage storage1{};
    ASSERT_EQ(Status::OK, create_storage(storage0));
    ASSERT_EQ(Status::OK, create_storage(storage1));
    std::string k("k");   // NOLINT
    std::string v0("v0"); // NOLINT
    std::string v1("v1"); // NOLINT
    Token token{};
    ASSERT_EQ(Status::OK, enter(token));
    ASSERT_EQ(Status::OK, upsert(token, storage0, k, v0));
    ASSERT_EQ(Status::OK, upsert(token, storage1, k, v0));
    ASSERT_EQ(Status::OK, commit(token)); // NOLINT
    ASSERT_EQ(Status::OK, upsert(token, storage0, k, v1));
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(token, storage1, k, vb));
    ASSERT_EQ(memcmp(vb.data(), v0.data(), v0.size()), 0);
    ASSERT_EQ(Status::OK, commit(token)); // NOLINT
}

} // namespace shirakami::testing