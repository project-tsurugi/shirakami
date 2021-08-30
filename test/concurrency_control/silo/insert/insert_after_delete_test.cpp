
#include <xmmintrin.h>

#include <bitset>

#include <glog/logging.h>
#include <gtest/gtest.h>


#ifdef WP

#include "concurrency_control/wp/include/epoch.h"

#else

#include "concurrency_control/silo/include/epoch.h"
#include "concurrency_control/silo/include/tuple_local.h"

#endif

#include "shirakami/interface.h"

namespace shirakami::testing {

using namespace shirakami;

Storage st;

class insert_after_delete : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/insert_after_delete_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(insert_after_delete, independent_tx) { // NOLINT
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
    while (Status::OK != insert(s, st, k, v)) {
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
    LOG(INFO) << "before_insert " << before_insert;
    LOG(INFO) << "after_insert " << after_insert;
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(insert_after_delete, same_tx) { // NOLINT
    register_storage(st);
    std::string k("k");   // NOLINT
    std::string v("v");   // NOLINT
    std::string v2("v2"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, st, k));
    ASSERT_EQ(Status::WARN_WRITE_TO_LOCAL_WRITE, insert(s, st, k, v2));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Tuple* tup{};
    ASSERT_EQ(Status::OK, search_key(s, st, k, &tup));
    ASSERT_EQ(memcmp(tup->get_value().data(), v2.data(), v2.size()), 0);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
}

} // namespace shirakami::testing
