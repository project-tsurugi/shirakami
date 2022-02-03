
#include <array>
#include <bitset>
#include <mutex>
#include <thread>

#include "compiler.h"
#include "memory.h"

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"


namespace shirakami::testing {

using namespace shirakami;

Storage st{};
class simple_upsert : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "upsert-simple_upsert_test");
        FLAGS_stderrthreshold = 0;        // output more than INFO
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/tmp/simple_upsert_test_log");
    }
    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(false, log_dir_); // NOLINT
        register_storage(st);
    }

    void TearDown() override {
        delete_storage(st);
        fin();
    }

private:
    static inline std::once_flag init_google_; // NOLINT
    static inline std::string log_dir_;        // NOLINT
};

TEST_F(simple_upsert, upsert) { // NOLINT
    std::string k("aaa");       // NOLINT
    std::string v("aaa");       // NOLINT
    std::string v2("bbb");      // NOLINT
    Token s{};
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::WARN_ALREADY_EXISTS, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, upsert(s, st, k, v2));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, search_key(s, st, k, tuple));
    std::string val{};
    tuple->get_value(val);
    ASSERT_EQ(memcmp(val.data(), v2.data(), v2.size()), 0);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_upsert, upsert_after_insert) { // NOLINT
    std::string k("K");
    std::string v("v");
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    ASSERT_EQ(Status::WARN_WRITE_TO_LOCAL_WRITE, upsert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
