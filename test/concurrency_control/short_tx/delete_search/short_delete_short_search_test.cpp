
#include <mutex>
#include <thread>

// test/include
#include "test_tool.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::testing {

class short_delete_short_search : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "short_delete_short_search_test");
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

TEST_F(short_delete_short_search, search_delete) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(upsert(s, st, "", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s, st, "", vb));
    ASSERT_EQ(vb, "");
    ASSERT_EQ(Status::OK, delete_record(s, st, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(short_delete_short_search, delete_search) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(upsert(s, st, "", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    {
        // stop gc
        std::unique_lock<std::mutex> lk{epoch::get_ep_mtx()};
        ASSERT_EQ(Status::OK, delete_record(s, st, ""));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
        std::string vb{};
        // this should find logical record deleted.
        ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, st, "", vb));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    }
    wait_epoch_update(); // start gc
    wait_epoch_update(); // wait process of gc thread
    {
        // not change behavior
        std::string vb{};
        // this should not find logical record deleted.
        ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, st, "", vb));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    }

    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing