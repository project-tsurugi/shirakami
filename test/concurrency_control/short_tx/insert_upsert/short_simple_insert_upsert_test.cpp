
#include <array>
#include <bitset>
#include <mutex>
#include <thread>

#include "compiler.h"
#include "memory.h"

#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"


namespace shirakami::testing {

using namespace shirakami;

Storage st{};
class simple_insert_upsert : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "upsert-simple_insert_upsert_test");
        FLAGS_stderrthreshold = 0;        // output more than INFO
    }
    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
        create_storage(st);
    }

    void TearDown() override {
        fin();
    }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(simple_insert_upsert, upsert_after_insert) { // NOLINT
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
