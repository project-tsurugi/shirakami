
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
class upsert_after_upsert : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "upsert-upsert_after_upsert_test");
        FLAGS_stderrthreshold = 0;        // output more than INFO
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/tmp/upsert_after_upsert_test_log");
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

TEST_F(upsert_after_upsert, double_upsert) { // NOLINT
    std::string k("aaa");                    // NOLINT
    std::string v("bbb");                    // NOLINT
    std::string v2("ccc");                   // NOLINT
    std::string v3("ddd");                   // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, upsert(s, st, k, v2));
    ASSERT_EQ(Status::WARN_WRITE_TO_LOCAL_WRITE, upsert(s, st, k, v3));
    std::string vb{};
    ASSERT_EQ(Status::WARN_READ_FROM_OWN_OPERATION, search_key(s, st, k, vb));
    ASSERT_EQ(memcmp(vb.data(), v3.data(), v3.size()), 0);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(upsert_after_upsert, manyUpsert) { // NOLINT
    std::string k("K");
    std::string v(1000, 'v'); // NOLINT
    Token s{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(s2));

    ASSERT_EQ(tx_begin(s2), Status::OK); // to block gc

    for (std::size_t i = 0; i < 1000; ++i) { // NOLINT
        ASSERT_EQ(Status::OK, upsert(s, st, k, v));
        ASSERT_EQ(Status::OK, commit(s));
    }

    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(s2));
    LOG(INFO) << "maxrss:\t" << getRusageRUMaxrss();
}

} // namespace shirakami::testing