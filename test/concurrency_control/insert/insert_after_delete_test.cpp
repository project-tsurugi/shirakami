#include <bitset>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "tuple_local.h"

#include "concurrency_control/include/epoch.h"

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
    while (Status::OK != insert(s, st, k, v)) {
        LOG_EVERY_N(INFO, 20) << "fail insert";
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
    std::string k("k"); // NOLINT
    std::string v("v"); // NOLINT
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
