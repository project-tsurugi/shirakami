
#include <bitset>
#include <mutex>
#include <thread>

#include "gtest/gtest.h"

// shirakami-impl interface library
#include "concurrency_control/silo/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

Storage storage;
class insert_update_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "insert_update-insert_update_test");
        FLAGS_stderrthreshold = 0;        // output more than INFO
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/tmp/insert_update_test_log");
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

TEST_F(insert_update_test, insert_update) { // NOLINT
    register_storage(storage);
    std::string k("k"); // NOLINT
    std::string v("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, update(s, storage, k, v));
    ASSERT_EQ(Status::WARN_UNIQUE_CONSTRAINT, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
