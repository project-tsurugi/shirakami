
#include <cmath>

#include <mutex>

#include <string_view>

#include "concurrency_control/include/ongoing_tx.h"

#include "storage.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

using namespace shirakami;
using namespace std::string_view_literals;

namespace shirakami::testing {

class storage_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-storage-storage_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
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
    ASSERT_EQ(Status::OK, create_storage("1", st));
    ASSERT_EQ(st, storage::initial_strg_ctr << 32); // NOLINT
    ASSERT_EQ(Status::OK, create_storage("2", st));
    ASSERT_EQ(st, (storage::initial_strg_ctr + 1) << 32); // NOLINT
    ASSERT_EQ(Status::OK, create_storage("3", st));
    ASSERT_EQ(st, (storage::initial_strg_ctr + 2) << 32); // NOLINT
    ASSERT_EQ(Status::OK, delete_storage(st)); // interrupt delete_storage
    ASSERT_EQ(Status::OK, create_storage("4", st));
    ASSERT_EQ(st,
              (storage::initial_strg_ctr + 3) // NOLINT
                      << 32); // number trend is not changed. // NOLINT
    // using string key
    // null key
    ASSERT_EQ(Status::OK, create_storage("", st));
    // not null key
    ASSERT_EQ(Status::OK, create_storage("NAUTI", st));

    // using large key > 30KB
    std::string large_key(1024 * 36, 'a'); // NOLINT
    ASSERT_EQ(Status::WARN_INVALID_KEY_LENGTH, create_storage(large_key, st));
}

TEST_F(storage_test, user_specified_storage_id_test) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("1", st, {1}));
    ASSERT_EQ(Status::OK, storage::exist_storage(st));
    ASSERT_EQ(Status::OK, storage::exist_storage(1));
    ASSERT_EQ(Status::OK, create_storage("2", st, {2}));
    ASSERT_EQ(Status::OK, storage::exist_storage(st));
    ASSERT_EQ(Status::OK, storage::exist_storage(2));
    ASSERT_EQ(Status::WARN_ALREADY_EXISTS, create_storage("3", st, {2}));
    ASSERT_EQ(
            Status::WARN_STORAGE_ID_DEPLETION,
            create_storage("4", st, {static_cast<std::uint64_t>(pow(2, 33))}));
}

TEST_F(storage_test, exist_storage_test) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, storage::exist_storage(st));
    ASSERT_EQ(Status::OK, create_storage("1", st));
    ASSERT_EQ(Status::OK, storage::exist_storage(st));
    ASSERT_EQ(Status::OK, delete_storage(st));
    ASSERT_EQ(Status::WARN_NOT_FOUND, storage::exist_storage(st));
    // using string key
    // null key
    ASSERT_EQ(Status::OK, create_storage("", st));
    ASSERT_EQ(Status::OK, storage::exist_storage(st));
    // not null key
    ASSERT_EQ(Status::OK, create_storage("NAUTI", st));
    ASSERT_EQ(Status::OK, storage::exist_storage(st));
}

TEST_F(storage_test, get_storage_key_test) { // NOLINT
    Storage st1{}, st2{};
    ASSERT_EQ(Status::OK, create_storage("1", st1));
    ASSERT_EQ(Status::OK, create_storage("2", st2));
    std::string sk1{}, sk2{};
    ASSERT_EQ(Status::OK, get_storage_key(st1, sk1));
    EXPECT_EQ("1", sk1);
    ASSERT_EQ(Status::OK, get_storage_key(st2, sk2));
    EXPECT_EQ("2", sk2);
}

TEST_F(storage_test, create_storage_generating_storage_key) { // NOLINT
    // test create_storage that automatically generates storage key
    Storage st1{}, st2{}, st3{}, st4{};
    ASSERT_EQ(Status::OK, create_storage(st1));
    ASSERT_EQ(st1, storage::initial_strg_ctr << 32); // NOLINT
    ASSERT_EQ(Status::OK, create_storage(st2));
    ASSERT_EQ(st2, (storage::initial_strg_ctr + 1) << 32); // NOLINT
    ASSERT_EQ(Status::OK, create_storage( st3));
    ASSERT_EQ(st3, (storage::initial_strg_ctr + 2) << 32); // NOLINT
    ASSERT_EQ(Status::OK, delete_storage(st3));
    ASSERT_EQ(Status::OK, create_storage(st4));
    ASSERT_EQ(st4, (storage::initial_strg_ctr + 3) << 32); // NOLINT

    // Verify generated storage key can be fetched via get_storage_key.
    // Current implementation is just big-endian byte sequence of shirakami::Storage.
    std::string sk1{}, sk2{}, sk3{}, sk4{};
    ASSERT_EQ(Status::OK, get_storage_key(st1, sk1));
    EXPECT_EQ("\x00\x00\x00\x01\x00\x00\x00\x00"sv, sk1);
    ASSERT_EQ(Status::OK, get_storage_key(st2, sk2));
    EXPECT_EQ("\x00\x00\x00\x02\x00\x00\x00\x00"sv, sk2);
    ASSERT_EQ(Status::WARN_NOT_FOUND, get_storage_key(st3, sk3));
    ASSERT_EQ(Status::OK, get_storage_key(st4, sk4));
    EXPECT_EQ("\x00\x00\x00\x04\x00\x00\x00\x00"sv, sk4);
}

} // namespace shirakami::testing
