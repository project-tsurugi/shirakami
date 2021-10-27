
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
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/search_upsert_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(search_upsert, simple) { // NOLINT
    Token s{};
    Storage storage{};
    ASSERT_EQ(register_storage(storage), Status::OK);
    ASSERT_EQ(enter(s), Status::OK);
    std::string k{"k"};
    std::string v{"v"};
    Tuple* tuple{};
    ASSERT_EQ(search_key(s, storage, k, tuple), Status::WARN_NOT_FOUND);
    ASSERT_EQ(upsert(s, storage, k, v), Status::OK);
    ASSERT_EQ(search_key(s, storage, k, tuple), Status::WARN_READ_FROM_OWN_OPERATION);
    ASSERT_EQ(memcmp(tuple->get_key().data(), k.data(), k.size()), 0);
    ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
    ASSERT_EQ(commit(s), Status::OK);
    ASSERT_EQ(search_key(s, storage, k, tuple), Status::OK);
    ASSERT_EQ(memcmp(tuple->get_key().data(), k.data(), k.size()), 0);
    ASSERT_EQ(memcmp(tuple->get_value().data(), v.data(), v.size()), 0);
    ASSERT_EQ(commit(s), Status::OK);
    ASSERT_EQ(leave(s), Status::OK);
}

} // namespace shirakami::testing
