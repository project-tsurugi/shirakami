
#include "concurrency_control/silo_variant/include/session_info.h"
#include "gtest/gtest.h"
#include "tuple_local.h"

#include "kvs/interface.h"

#ifdef CC_SILO_VARIANT
using namespace shirakami::cc_silo_variant;
#endif

namespace shirakami::testing {

using namespace shirakami::cc_silo_variant;

class session_info_test : public ::testing::Test {
public:
    void SetUp() override { init(); }  // NOLINT

    void TearDown() override { fin(); }
};

TEST_F(session_info_test, get_txbegan_) {  // NOLINT
    std::string k("aaa");              // NOLINT
    std::string v("bbb");              // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    auto* ti = static_cast<session_info*>(s);
    ASSERT_EQ(ti->get_txbegan(), false);
    // test upsert
    ASSERT_EQ(Status::OK, upsert(s, k, v));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    ASSERT_EQ(Status::OK, upsert(s, k, v));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    // test delete
    ASSERT_EQ(Status::OK, delete_record(s, k));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    ASSERT_EQ(Status::OK, delete_record(s, k));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    // test insert
    ASSERT_EQ(Status::OK, insert(s, k, v));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    ASSERT_EQ(Status::OK, insert(s, k, v));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    // test update
    ASSERT_EQ(Status::OK, update(s, k, v));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    ASSERT_EQ(Status::OK, update(s, k, v));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    // test search
    Tuple* tuple = {};
    ASSERT_EQ(Status::OK, search_key(s, k, &tuple));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    ASSERT_EQ(Status::OK, search_key(s, k, &tuple));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    // test scan
    std::vector<const Tuple*> res;
    ASSERT_EQ(Status::OK, scan_key(s, k, scan_endpoint::INCLUSIVE, k, scan_endpoint::INCLUSIVE, res));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    ASSERT_EQ(Status::OK, scan_key(s, k, scan_endpoint::INCLUSIVE, k, scan_endpoint::INCLUSIVE, res));
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    // test open_scan
    ScanHandle handle = {};
    ASSERT_EQ(Status::OK, open_scan(s, k, scan_endpoint::INCLUSIVE, k, scan_endpoint::INCLUSIVE, handle));
    ASSERT_EQ(handle, 0);
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_txbegan(), false);
    ASSERT_EQ(Status::OK, open_scan(s, k, scan_endpoint::INCLUSIVE, k, scan_endpoint::INCLUSIVE, handle));
    ASSERT_EQ(handle, 0);
    ASSERT_EQ(ti->get_txbegan(), true);
    ASSERT_EQ(Status::OK, commit(s));
    delete_all_records();
    ASSERT_EQ(Status::OK, leave(s));
}

}  // namespace shirakami::testing