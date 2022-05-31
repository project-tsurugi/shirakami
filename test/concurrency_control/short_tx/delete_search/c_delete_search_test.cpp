
#include <mutex>
#include <thread>

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::testing {

class delete_search : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-silo-delete_search_test");
        FLAGS_stderrthreshold = 0;        // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(false, "/tmp/shirakami_c_delete_search_test"); // NOLINT
        register_storage(st_);
    }

    void TearDown() override { fin(); }

    [[nodiscard]] Storage get_st() const { return st_; }

private:
    Storage st_{};
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(delete_search, search_delete) { // NOLINT
    Storage st{get_st()};
    std::string k("k"); // NOLINT
    std::string v("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(upsert(s, st, k, v), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s, st, k, vb));
    ASSERT_EQ(vb, v);
    ASSERT_EQ(Status::OK, delete_record(s, st, k));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(delete_search, delete_search_) { // NOLINT
    Storage st{get_st()};
    std::string k("k"); // NOLINT
    std::string v("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(upsert(s, st, k, v), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, st, k));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::string vb{};
    auto rc{search_key(s, st, k, vb)};
    if (rc != Status::WARN_NOT_FOUND            // may BUILD_WP=OFF
        && rc != Status::WARN_CONCURRENT_DELETE // BUILD_WP=ON
    ) {
        ASSERT_EQ(true, false);
    } else {
        LOG(INFO) << rc;
    }
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing