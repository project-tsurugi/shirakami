
#include "concurrency_control/include/session_info.h"
#include "tuple_local.h"
#include "gtest/gtest.h"

#include "shirakami/interface.h"

namespace shirakami::testing {

using namespace shirakami;

class session_info_test : public ::testing::Test {
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/session_info_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(session_info_test, get_txbegan_) { // NOLINT
    Storage storage;
    register_storage(storage);
    std::string k("aaa"); // NOLINT
    std::string v("bbb"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    auto* ti = static_cast<session_info*>(s);
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
#ifdef CPR
    while (Status::OK != search_key(s, storage, k, &tuple)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, search_key(s, storage, k, &tuple));
#endif
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_txbegan(), false);
#ifdef CPR
    while (Status::OK != search_key(s, storage, k, &tuple)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, search_key(s, storage, k, &tuple));
#endif
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    // test scan
    std::vector<const Tuple*> res;
#if defined(CPR)
    while (Status::OK != scan_key(s, storage, k, scan_endpoint::INCLUSIVE, k, scan_endpoint::INCLUSIVE, res)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::INCLUSIVE, k, scan_endpoint::INCLUSIVE, res));
#endif
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_txbegan(), false);
#if defined(CPR)
    while (Status::OK != scan_key(s, storage, k, scan_endpoint::INCLUSIVE, k, scan_endpoint::INCLUSIVE, res)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::INCLUSIVE, k, scan_endpoint::INCLUSIVE, res));
#endif
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
