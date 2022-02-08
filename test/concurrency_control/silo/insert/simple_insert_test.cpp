
#include <bitset>
#include <mutex>
#include <thread>

#include "concurrency_control/silo/include/record.h"

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

Storage storage;
class simple_insert : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "insert-simple_insert_test");
        FLAGS_stderrthreshold = 0; // output more than INFO
        log_dir_ = MAC2STR(PROJECT_ROOT);
        log_dir_.append("/tmp/simple_insert_test_log");
    }
    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(false, log_dir_); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
    static inline std::string log_dir_;        // NOLINT
};

TEST_F(simple_insert, insert) { // NOLINT
    register_storage(storage);
    std::string k("aaa"); // NOLINT
    std::string v("bbb"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::WARN_ALREADY_EXISTS, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    std::string_view st_view{reinterpret_cast<char*>(&storage), // NOLINT
                             sizeof(storage)};
    auto check_records = [st_view](std::string_view key_view,
                                   std::string_view value_view) {
        Record** rec_d_ptr{
                std::get<0>(yakushima::get<Record*>(st_view, key_view))};
        ASSERT_NE(rec_d_ptr, nullptr);
        Record* rec_ptr{*rec_d_ptr};
        ASSERT_NE(rec_ptr, nullptr);
        {
            Tuple& tuple_ref{rec_ptr->get_tuple()};
            std::string key{};
            tuple_ref.get_key(key);
            ASSERT_EQ(memcmp(key.data(), key_view.data(), key_view.size()), 0);
            std::string read_value{};
            tuple_ref.get_value(read_value);
            ASSERT_EQ(memcmp(read_value.data(), value_view.data(),
                             value_view.size()),
                      0);
        }
    };

    check_records(k, v);

    // insert one char (0) records.
    char k2 = 0;
    std::string_view key_view{&k2, sizeof(k2)};
    ASSERT_EQ(Status::OK, insert(s, storage, key_view, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    check_records(key_view, v);

    // insert null key records.
    ASSERT_EQ(Status::OK, insert(s, storage, "", v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    check_records("", v);

    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_insert, long_value_insert) { // NOLINT
    register_storage(storage);
    std::string k("CUSTOMER"); // NOLINT
    std::string v(             // NOLINT
            "b23456789012345678901234567890123456789012345678901234567890123456"
            "7890"
            "12"
            "345678901234567890123456789012345678901234567890123456789012345678"
            "9012"
            "34"
            "567890123456789012345678901234567890123456789012345678901234567890"
            "1234"
            "56"
            "789012345678901234567890123456789012345678901234567890123456789012"
            "3456"
            "78"
            "901234567890123456789012345678901234567890123456789012345678901234"
            "5678"
            "90"
            "123456789012345678901234567890123456789012345678901234567890123456"
            "7890"
            "12"
            "345678901234567890123456789012345678901234567890123456789012345678"
            "9012"
            "34"
            "567890123456789012345678901234567890123456789012345678901234567890"
            "1234"
            "56"
            "789012345678901234567890123456789012345678901234567890123456789012"
            "3456"
            "78"
            "901234567890123456789012345678901234567890123456789012345678901234"
            "5678"
            "90"
            "123456789012345678901234567890123456789012345678901234567890123456"
            "7890"
            "12"
            "345678901234567890123456789012345678901234567890123456789012345678"
            "9012"
            "34"
            "5678901234567890123456789012345678901234567890");
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_insert, long_key_insert) { // NOLINT
    register_storage(storage);
    std::string k(56, '0'); // NOLINT
    k += "a";
    std::string v("v");
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s, storage, k, vb));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}
} // namespace shirakami::testing
