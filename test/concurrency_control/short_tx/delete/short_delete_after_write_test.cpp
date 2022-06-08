
#include <bitset>
#include <mutex>

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

Storage storage;
class delete_after_write : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "delete_delete_after_write_test");
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(delete_after_write, delete_after_insert) { // NOLINT
    register_storage(storage);
    std::string k1("k"); // NOLINT
    std::string v1("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k1, v1));
    ASSERT_EQ(Status::WARN_CANCEL_PREVIOUS_INSERT,
              delete_record(s, storage, k1));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(delete_after_write, delete_after_upsert) { // NOLINT
    register_storage(storage);
    std::string k1("k"); // NOLINT
    std::string v1("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, storage, k1, v1));
    ASSERT_EQ(Status::WARN_CANCEL_PREVIOUS_INSERT,
              delete_record(s, storage, k1));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
