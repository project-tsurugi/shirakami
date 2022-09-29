
#include <cmath>

#include <mutex>

#include "concurrency_control/wp/include/ongoing_tx.h"

#include "storage.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::testing {

class storage_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-storage-storage_test");
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

TEST_F(storage_test, get_storage_test) { // NOLINT
    Storage st{};
    // using string key
    // not null key
    ASSERT_EQ(Status::OK, create_storage("NAUTI", st));
    Storage st2{};
    ASSERT_EQ(Status::OK, get_storage("NAUTI", st2));
    ASSERT_EQ(st, st2);
    ASSERT_EQ(Status::OK, delete_storage(st));
    ASSERT_EQ(Status::WARN_NOT_FOUND, get_storage("NAUTI", st2));
    // null key
    ASSERT_EQ(Status::OK, create_storage("", st));
    ASSERT_EQ(Status::OK, get_storage("", st2));
    ASSERT_EQ(st, st2);
    ASSERT_EQ(Status::OK, delete_storage(st));
    ASSERT_EQ(Status::WARN_NOT_FOUND, get_storage("", st2));
}

TEST_F(storage_test, delete_storage_test) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::WARN_INVALID_HANDLE, delete_storage(st));
    ASSERT_EQ(Status::OK, create_storage("1", st));
    ASSERT_EQ(Status::OK, delete_storage(st));
    ASSERT_EQ(Status::WARN_INVALID_HANDLE, delete_storage(st));
    // using string key
    // not null key
    ASSERT_EQ(Status::OK, create_storage("NAUTI", st));
    // delete storage used storage key
    ASSERT_EQ(Status::OK, delete_storage(st));
    // null key
    ASSERT_EQ(Status::OK, create_storage("", st));
    // delete storage used storage null key
    ASSERT_EQ(Status::OK, delete_storage(st));
}

TEST_F(storage_test, list_storage_test) { // NOLINT
    std::vector<std::string> st_list{};
    ASSERT_EQ(Status::OK, list_storage(st_list));
    ASSERT_EQ(st_list.size(), 0);
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("1", st));
    ASSERT_EQ(Status::OK, list_storage(st_list));
    ASSERT_EQ(st_list.size(), 1);
    ASSERT_EQ(Status::OK, create_storage("2", st));
    ASSERT_EQ(Status::OK, list_storage(st_list));
    ASSERT_EQ(st_list.size(), 2);
    ASSERT_EQ(Status::OK, delete_storage(st)); // interrupt delete_storage
    ASSERT_EQ(Status::OK, list_storage(st_list));
    ASSERT_EQ(st_list.size(), 1);
}

} // namespace shirakami::testing