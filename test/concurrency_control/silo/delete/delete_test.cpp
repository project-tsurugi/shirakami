#include <bitset>

#include "concurrency_control/include/tuple_local.h"

#include "gtest/gtest.h"

#include "shirakami/interface.h"

namespace shirakami::testing {

using namespace shirakami;

Storage storage;
class delete_test : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/delete_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(delete_test, delete_) { // NOLINT
    register_storage(storage);
    std::string k("aaa");  // NOLINT
    std::string v("aaa");  // NOLINT
    std::string v2("bbb"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::WARN_NOT_FOUND, delete_record(s, storage, k));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, upsert(s, storage, k, v2));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, delete_record(s, storage, k));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::WARN_NOT_FOUND, delete_record(s, storage, k));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(delete_test, all_deletes) { // NOLINT
    register_storage(storage);
    std::string k("testing_a0123456"); // NOLINT
    std::string v("bbb");              // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::vector<const Tuple*> records{};
    ASSERT_EQ(Status::OK, scan_key(s, storage, "", scan_endpoint::INF, "",
                                   scan_endpoint::INF, records));
    for (auto&& t : records) {
        std::string key{};
        t->get_key(key);
        ASSERT_EQ(Status::OK, delete_record(s, storage, key));
    }
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
