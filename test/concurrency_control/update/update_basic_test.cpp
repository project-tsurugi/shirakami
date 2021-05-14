#include <bitset>
#include <future>

#include "gtest/gtest.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/record.h"
#include "concurrency_control/include/snapshot_manager.h"

#include "clock.h"
#include "logger.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

using namespace shirakami::logger;

namespace shirakami::testing {

using namespace shirakami;

class simple_update : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/test_log/update_basic_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(simple_update, update) { // NOLINT
    Storage storage;
    register_storage(storage);
    std::string k("aaa");  // NOLINT
    std::string v("aaa");  // NOLINT
    std::string v2("bbb"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::WARN_NOT_FOUND, update(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, search_key(s, storage, k, &tuple));
    ASSERT_EQ(
            memcmp(tuple->get_value().data(), v.data(), tuple->get_value().size()),
            0);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, update(s, storage, k, v2));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, search_key(s, storage, k, &tuple));
    ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
