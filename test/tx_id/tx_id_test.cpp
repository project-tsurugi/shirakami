
#include <mutex>
#include <string>

#include "concurrency_control/include/tid.h"
#include "shirakami/interface.h"
#include "gtest/gtest.h"
#include "glog/logging.h"
#include "atomic_wrapper.h"
#include "shirakami/api_storage.h"
#include "shirakami/api_tx_id.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"

namespace shirakami::testing {

using namespace shirakami;

class tx_id_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-tx_id-tx_id_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init();
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(tx_id_test, simple) { // NOLINT
                             // prepare
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s{};
    Token s1{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(s1));

    // test
    ASSERT_EQ(Status::OK, tx_begin({s}));
    ASSERT_EQ(Status::OK, tx_begin({s1}));
    std::string out_id{};
    ASSERT_EQ(Status::OK, get_tx_id(s, out_id));
    LOG(INFO) << out_id;
    ASSERT_EQ(Status::OK, get_tx_id(s1, out_id));
    // different token has different session id.
    LOG(INFO) << out_id;
    ASSERT_EQ(Status::OK, commit(s));  // NOLINT
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    ASSERT_EQ(Status::OK, tx_begin({s}));
    ASSERT_EQ(Status::OK, get_tx_id(s, out_id));
    // This value is incremented each time a transaction is processed.
    LOG(INFO) << out_id;
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ASSERT_EQ(Status::OK, tx_begin({s}));
    ASSERT_EQ(Status::OK, get_tx_id(s, out_id));
    // This value is incremented each time a transaction is processed.
    LOG(INFO) << out_id;

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(s1));
}

// moved to tid_word_test

} // namespace shirakami::testing
