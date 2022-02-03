#include <xmmintrin.h>

#include <array>
#include <bitset>

// apt
#include <glog/logging.h>

#include "compiler.h"

#include "concurrency_control/silo/include/epoch.h"

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

// third party
#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

Storage st{};

class upsert_after_delete : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/upsert_after_delete_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
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
    Tuple* tup{};
    ASSERT_EQ(Status::OK, search_key(s, st, k, tup));
    std::string val{};
    tup->get_value(val);
    ASSERT_EQ(memcmp(val.data(), v2.data(), v2.size()), 0);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
}

} // namespace shirakami::testing
