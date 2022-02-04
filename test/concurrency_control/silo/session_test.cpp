
#include "gtest/gtest.h"

#include "concurrency_control/silo/include/session.h"

#include "concurrency_control/include/tuple_local.h" // sizeof(Tuple)

#include "shirakami/interface.h"

namespace shirakami::testing {

using namespace shirakami;

class session_test : public ::testing::Test {
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/session_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(session_test, read_only) { // NOLINT
    Storage storage{};
    register_storage(storage);
    std::string k("aaa"); // NOLINT
    std::string v("bbb"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    auto* ti = static_cast<session*>(s);
    tx_begin(s, true);
    ASSERT_EQ(ti->get_read_only(), true);
    ASSERT_EQ(leave(s), Status::OK);
}

TEST_F(session_test, get_txbegan_) { // NOLINT
    Storage storage{};
    register_storage(storage);
    std::string k("aaa"); // NOLINT
    std::string v("bbb"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    auto* ti = static_cast<session*>(s);
    ASSERT_EQ(ti->get_txbegan(), false);
    // test upsert
    ASSERT_EQ(Status::OK, upsert(s, storage, k, v));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    ASSERT_EQ(Status::OK, upsert(s, storage, k, v));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    // test delete
    ASSERT_EQ(Status::OK, delete_record(s, storage, k));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    ASSERT_EQ(Status::OK, delete_record(s, storage, k));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    // test insert
    while (Status::OK != insert(s, storage, k, v)) {
        ;
    }
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    // test update
    ASSERT_EQ(Status::OK, update(s, storage, k, v));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    ASSERT_EQ(Status::OK, update(s, storage, k, v));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    // test search
    Tuple* tuple = {};
    ASSERT_EQ(Status::OK, search_key(s, storage, k, tuple));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    ASSERT_EQ(Status::OK, search_key(s, storage, k, tuple));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    // test open_scan
    ScanHandle handle = {};
    ASSERT_EQ(Status::OK, open_scan(s, storage, k, scan_endpoint::INCLUSIVE, k, scan_endpoint::INCLUSIVE, handle));
    ASSERT_EQ(handle, 0);
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    ASSERT_EQ(Status::OK, open_scan(s, storage, k, scan_endpoint::INCLUSIVE, k, scan_endpoint::INCLUSIVE, handle));
    ASSERT_EQ(handle, 0);
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, commit(s));
    delete_all_records();
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
