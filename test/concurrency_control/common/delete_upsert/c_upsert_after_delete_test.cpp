#include <xmmintrin.h>

#include <array>
#include <bitset>
#include <mutex>

#include "compiler.h"

#ifdef WP

#include "concurrency_control/wp/include/epoch.h"

#else

#include "concurrency_control/silo/include/epoch.h"

#endif

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

Storage st{};

class upsert_after_delete : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "upsert-upsert_after_delete_test.cpp");
        FLAGS_stderrthreshold = 0;
        log_dir_ = MAC2STR(PROJECT_ROOT); //NOLINT
        log_dir_.append("/tmp/upsert_after_delete_test_log");
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

TEST_F(upsert_after_delete, upsert) { // NOLINT
    register_storage(st);
    std::string k("k"); // NOLINT
    std::string v("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, delete_record(s, st, k));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    epoch::epoch_t before_upsert{epoch::get_global_epoch()};
    //std::size_t ctr{0};
    while (Status::OK != upsert(s, st, k, v)) {
        //LOG_EVERY_N(INFO, 20) << "fail insert"; // NOLINT
        // ^^ NOLINT does not work on Ubuntu18.04. It works in 20.04.
        //if (ctr % 30 == 0) { // NOLINT
        //    LOG(INFO) << "fail insert";
        //}
        //++ctr;
        _mm_pause();
    }
    epoch::epoch_t after_upsert{epoch::get_global_epoch()};
    LOG(INFO) << "before_upsert " << before_upsert;
    LOG(INFO) << "after_upsert " << after_upsert;
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(upsert_after_delete, same_tx) { // NOLINT
    register_storage(st);
    std::string k("k");   // NOLINT
    std::string v("v");   // NOLINT
    std::string v2("v2"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, st, k));
    ASSERT_EQ(Status::WARN_WRITE_TO_LOCAL_WRITE, upsert(s, st, k, v2));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s, st, k, vb));
    ASSERT_EQ(memcmp(vb.data(), v2.data(), v2.size()), 0);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
}

} // namespace shirakami::testing