
#include <bitset>
#include <mutex>
#include <thread>

#include "gtest/gtest.h"

#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class insert_update_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "insert_update-insert_update_test");
        FLAGS_stderrthreshold = 0; // output more than INFO
    }
    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(insert_update_test, insert_update) { // NOLINT
    // prepare
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // test
    ASSERT_EQ(Status::OK, insert(s, st, "", "v"));
    ASSERT_EQ(Status::OK, update(s, st, "", "v1"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    
    // verify
    std::string buf{};
    ASSERT_EQ(Status::OK, search_key(s, st, "", buf));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(buf, "v1");

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(insert_update_test, update_insert) { // NOLINT
    // prepare
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ASSERT_EQ(Status::OK, update(s, st, "", "v"));
    ASSERT_EQ(Status::WARN_ALREADY_EXISTS, insert(s, st, "", "v1"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    
    // verify
    std::string buf{};
    ASSERT_EQ(Status::OK, search_key(s, st, "", buf));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(buf, "v");

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing