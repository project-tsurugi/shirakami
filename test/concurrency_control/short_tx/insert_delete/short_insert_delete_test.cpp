
#include <bitset>
#include <mutex>
#include <thread>

#include "gtest/gtest.h"

#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

Storage storage;
class insert_delete_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "insert_delete-insert_delete_test");
        FLAGS_stderrthreshold = 0;        // output more than INFO
    }
    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(insert_delete_test, insert_delete) { // NOLINT
    register_storage(storage);
    std::string k("k"); // NOLINT
    std::string v("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::WARN_CANCEL_PREVIOUS_INSERT, delete_record(s, storage, k));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // verify
    std::string vb{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, storage, k, vb));

    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(insert_delete_test, insert_delete_insert) { // NOLINT
    register_storage(storage);
    std::string k("k"); // NOLINT
    std::string v("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::WARN_CANCEL_PREVIOUS_INSERT, delete_record(s, storage, k));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
}

} // namespace shirakami::testing
