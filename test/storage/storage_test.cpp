
#include <math.h>

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

TEST_F(storage_test, create_storage_test) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage(st));
    ASSERT_EQ(st, storage::initial_strg_ctr << 32);
    ASSERT_EQ(Status::OK, create_storage(st));
    ASSERT_EQ(st, (storage::initial_strg_ctr + 1) << 32);
    ASSERT_EQ(Status::OK, create_storage(st));
    ASSERT_EQ(st, (storage::initial_strg_ctr + 2) << 32);
    ASSERT_EQ(Status::OK, delete_storage(st)); // interrupt delete_storage
    ASSERT_EQ(Status::OK, create_storage(st));
    ASSERT_EQ(st,
              (storage::initial_strg_ctr + 3)
                      << 32); // number trend is not changed.
}

TEST_F(storage_test, user_specified_storage_id_test) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage(st, {1}));
    ASSERT_EQ(Status::OK, exist_storage(st));
    ASSERT_EQ(Status::OK, exist_storage(1));
    ASSERT_EQ(Status::OK, create_storage(st, {2}));
    ASSERT_EQ(Status::OK, exist_storage(st));
    ASSERT_EQ(Status::OK, exist_storage(2));
    ASSERT_EQ(Status::WARN_ALREADY_EXISTS, create_storage(st, {2}));
    ASSERT_EQ(Status::WARN_STORAGE_ID_DEPLETION,
              create_storage(st, {static_cast<std::uint64_t>(pow(2, 33))}));
}

TEST_F(storage_test, exist_storage_test) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, exist_storage(st));
    ASSERT_EQ(Status::OK, create_storage(st));
    ASSERT_EQ(Status::OK, exist_storage(st));
    ASSERT_EQ(Status::OK, delete_storage(st));
    ASSERT_EQ(Status::WARN_NOT_FOUND, exist_storage(st));
}

TEST_F(storage_test, delete_storage_test) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::WARN_INVALID_HANDLE, delete_storage(st));
    ASSERT_EQ(Status::OK, create_storage(st));
    ASSERT_EQ(Status::OK, delete_storage(st));
    ASSERT_EQ(Status::WARN_INVALID_HANDLE, delete_storage(st));
}

TEST_F(storage_test, list_storage_test) { // NOLINT
    std::vector<Storage> st_list{};
    ASSERT_EQ(Status::OK, list_storage(st_list));
    ASSERT_EQ(st_list.size(), 0);
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage(st));
    ASSERT_EQ(Status::OK, list_storage(st_list));
    ASSERT_EQ(st_list.size(), 1);
    ASSERT_EQ(Status::OK, create_storage(st));
    ASSERT_EQ(Status::OK, list_storage(st_list));
    ASSERT_EQ(st_list.size(), 2);
    ASSERT_EQ(Status::OK, delete_storage(st)); // interrupt delete_storage
    ASSERT_EQ(Status::OK, list_storage(st_list));
    ASSERT_EQ(st_list.size(), 1);
}

} // namespace shirakami::testing