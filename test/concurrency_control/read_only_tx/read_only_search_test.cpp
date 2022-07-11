
#include <array>
#include <mutex>

#include "test_tool.h"

#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class read_only_search_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "helper-read_only_search_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(read_only_search_test, search_SS_version) { // NOLINT
    // =================================
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage(st));

    Token s{};
    Token s1{};
    Token s2{};
    Token s3{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK, enter(s3));

    ASSERT_EQ(Status::OK, upsert(s, st, "", "v1"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, tx_begin({s1, transaction_options::transaction_type::LONG}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, upsert(s, st, "", "v2"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, tx_begin({s2, transaction_options::transaction_type::LONG}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, upsert(s, st, "", "v3"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, tx_begin({s3, transaction_options::transaction_type::READ_ONLY}));
    wait_epoch_update();
    // =================================

    // =================================
    // test phase
    std::string buf{};
    ASSERT_EQ(Status::OK, search_key(s1, st, "", buf));
    ASSERT_EQ(buf, "v1");
    ASSERT_EQ(Status::OK, search_key(s2, st, "", buf));
    ASSERT_EQ(buf, "v2");
    ASSERT_EQ(Status::OK, search_key(s3, st, "", buf));
    ASSERT_EQ(buf, "v1"); // read SS
    // =================================

    // =================================
    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
    ASSERT_EQ(Status::OK, leave(s3));
}

} // namespace shirakami::testing