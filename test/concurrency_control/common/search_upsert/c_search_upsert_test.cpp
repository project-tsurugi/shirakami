
#include <array>
#include <mutex>

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class search_upsert : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-common-"
                                  "search_upsert-search_upsert_test");
        FLAGS_stderrthreshold = 0;
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/tmp/search_upsert_test_log");
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

TEST_F(search_upsert, simple) { // NOLINT
    Token s{};
    Storage storage{};
    ASSERT_EQ(register_storage(storage), Status::OK);
    ASSERT_EQ(enter(s), Status::OK);
    std::string k{"k"};
    std::string v{"v"};
    std::string vb{};
    ASSERT_EQ(search_key(s, storage, k, vb), Status::WARN_NOT_FOUND);
    ASSERT_EQ(upsert(s, storage, k, v), Status::OK);
    ASSERT_EQ(search_key(s, storage, k, vb),
              Status::WARN_READ_FROM_OWN_OPERATION);
    ASSERT_EQ(memcmp(vb.data(), v.data(), v.size()), 0);
    ASSERT_EQ(commit(s), Status::OK);
    ASSERT_EQ(search_key(s, storage, k, vb), Status::OK);
    ASSERT_EQ(memcmp(vb.data(), v.data(), v.size()), 0);
    ASSERT_EQ(commit(s), Status::OK);
    ASSERT_EQ(leave(s), Status::OK);
}

} // namespace shirakami::testing
