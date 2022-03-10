
#include <bitset>
#include <mutex>

#include "gtest/gtest.h"

#include "concurrency_control/include/tuple_local.h" // sizeof(Tuple)

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

Storage storage;

class single_search : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "search-single_search_test");
        FLAGS_stderrthreshold = 0;
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/tmp/single_search_test_log");
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

TEST_F(single_search, search_at_non_existing_storage) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::string vb{};
    ASSERT_EQ(Status::WARN_STORAGE_NOT_FOUND, search_key(s, {}, "", vb));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(single_search, search) { // NOLINT
    register_storage(storage);
    std::string k("aaa"); // NOLINT
    std::string v("bbb"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::string vb{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, storage, k, vb));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, search_key(s, storage, k, vb));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
