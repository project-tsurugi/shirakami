#include <xmmintrin.h>

#include <bitset>
#include <future>

#include <glog/logging.h>

#ifdef WP

#include "concurrency_control/wp/include/epoch.h"

#else

#include "concurrency_control/silo/include/epoch.h"

#endif
#include "shirakami/interface.h"

#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

Storage st;

class update_after_delete : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/test_log/update_after_delete_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(update_after_delete, independent_tx) { // NOLINT
    register_storage(st);
    std::string k("k"); // NOLINT
    std::string v("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, st, k));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    epoch::epoch_t before_insert{epoch::get_global_epoch()};
    std::size_t ctr{0};
    while (Status::WARN_NOT_FOUND != update(s, st, k, v)) {
        //LOG_EVERY_N(INFO, 20) << "fail insert"; // NOLINT
        // ^^ NOLINT does not work on Ubuntu18.04. It works in 20.04.
        if (ctr % 30 == 0) { // NOLINT
            LOG(INFO) << "fail insert";
        }
        ++ctr;
        _mm_pause();
        ;
    }
    epoch::epoch_t after_insert{epoch::get_global_epoch()};
    LOG(INFO) << "before_update " << before_insert;
    LOG(INFO) << "after_update " << after_insert;
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(update_after_delete, same_tx) { // NOLINT
    register_storage(st);
    std::string k("k");   // NOLINT
    std::string v("v");   // NOLINT
    std::string v2("v2"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, st, k));
    ASSERT_EQ(Status::WARN_ALREADY_DELETE, update(s, st, k, v2));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
}

} // namespace shirakami::testing
