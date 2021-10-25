
#include <mutex>
#include <thread>

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::testing {

class read_write : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-silo-read_write_test");
        FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/read_write_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(read_write, point_read_update) { // NOLINT
    Storage storage{};
    ASSERT_EQ(Status::OK, register_storage(storage));
    std::string k("k");   // NOLINT
    std::string v("v");   // NOLINT
    std::string v2("v2"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(upsert(s, storage, k, v), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, search_key(s, storage, k, tuple));
    ASSERT_EQ(Status::OK, update(s, storage, k, v2));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
}

TEST_F(read_write, point_read_delete) { // NOLINT
    Storage storage{};
    ASSERT_EQ(Status::OK, register_storage(storage));
    std::string k("k"); // NOLINT
    std::string v("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(upsert(s, storage, k, v), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, search_key(s, storage, k, tuple));
    ASSERT_EQ(Status::OK, delete_record(s, storage, k));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
}

TEST_F(read_write, range_read_insert) { // NOLINT
    Storage storage{};
    ASSERT_EQ(Status::OK, register_storage(storage));
    std::string k("k");   // NOLINT
    std::string k2("k2"); // NOLINT
    std::string v("v");   // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(upsert(s, storage, k, v), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::vector<const Tuple*> records{};
    ASSERT_EQ(Status::OK, scan_key(s, storage, "", scan_endpoint::INF, "",
                                   scan_endpoint::INF, records));
    ASSERT_EQ(insert(s, storage, k2, v), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
}

TEST_F(read_write, range_read_delete) { // NOLINT
    Storage storage{};
    ASSERT_EQ(Status::OK, register_storage(storage));
    std::string k("k");   // NOLINT
    std::string k2("k2"); // NOLINT
    std::string v("v");   // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(upsert(s, storage, k, v), Status::OK);
    ASSERT_EQ(upsert(s, storage, k2, v), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::vector<const Tuple*> records{};
    ASSERT_EQ(Status::OK, scan_key(s, storage, "", scan_endpoint::INF, "",
                                   scan_endpoint::INF, records));
    ASSERT_EQ(Status::OK, delete_record(s, storage, k));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
}
} // namespace shirakami::testing