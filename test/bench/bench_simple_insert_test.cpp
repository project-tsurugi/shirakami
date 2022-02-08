
#include <bitset>
#include <mutex>

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "bench/include/gen_key.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

Storage storage;
class simple_insert : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-bench-bench_simple_insert_test");
        FLAGS_stderrthreshold = 0;
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/build/bench_simple_insert_test_log");
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

TEST_F(simple_insert, long_key_insert) { // NOLINT
    register_storage(storage);
    std::size_t key_length = 8; // NOLINT
    constexpr std::size_t key_num = 3;
    std::string v("v");
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, make_key(key_length, key_num), v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::string vb{};
    ASSERT_EQ(Status::OK,
              search_key(s, storage, make_key(key_length, key_num), vb));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    key_length = 64; // NOLINT
    ASSERT_EQ(Status::OK, insert(s, storage, make_key(key_length, key_num), v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              search_key(s, storage, make_key(key_length, key_num), vb));
    std::string str_key = make_key(key_length, key_num);
    ASSERT_EQ(Status::OK, search_key(s, storage, str_key, vb));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ASSERT_EQ(Status::OK, leave(s));
}
} // namespace shirakami::testing
