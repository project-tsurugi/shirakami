
#include <bitset>
#include <mutex>
#include <thread>

#ifdef WP

#include "concurrency_control/wp/include/record.h"

#else

#include "concurrency_control/silo/include/record.h"

#endif

#include "concurrency_control/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

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
    }
    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(false, "/tmp/shirakami_c_simple_insert_test"); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
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

    auto check_records = [](std::string_view key_view,
                            std::string_view value_view) {
        Record* rec_ptr{};
        ASSERT_EQ(Status::OK, get<Record>(storage, key_view, rec_ptr));
        {
            std::string key{};
            rec_ptr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), key_view.data(), key_view.size()), 0);
            std::string read_value{};
            rec_ptr->get_value(read_value);
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

} // namespace shirakami::testing
