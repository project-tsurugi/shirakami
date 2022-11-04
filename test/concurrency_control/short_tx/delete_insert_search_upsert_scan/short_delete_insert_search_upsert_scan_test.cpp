
#include <bitset>
#include <mutex>

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class short_delete_insert_search_upsert_scan
    : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-short_tx-"
                                  "delete_insert_search_upsert_scan/"
                                  "delete_insert_search_upsert_scan_test");
        FLAGS_stderrthreshold = 0;
    }
    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};


TEST_F(short_delete_insert_search_upsert_scan, // NOLINT
       insert_delete_upsert_scan) {            // NOLINT
    Storage storage{};
    create_storage("", storage);
    std::string k("k");   // NOLINT
    std::string v("v");   // NOLINT
    std::string k2("k2"); // NOLINT
    std::string v2("v2"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k2, v2));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    std::string vb{}; // NOLINT
    ASSERT_EQ(Status::OK, search_key(s, storage, k, vb));
    ASSERT_EQ(vb, v);
    ASSERT_EQ(Status::WARN_CANCEL_PREVIOUS_INSERT,
              delete_record(s, storage, k));
    ASSERT_EQ(Status::OK, upsert(s, storage, k, v));

    ScanHandle handle{};
    ASSERT_EQ(Status::OK, open_scan(s, storage, k, scan_endpoint::INCLUSIVE, "",
                                    scan_endpoint::INF, handle));
    std::string sb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
    EXPECT_EQ(sb, "k"); // NOLINT
    EXPECT_EQ(Status::OK, close_scan(s, handle));
    EXPECT_EQ(Status::OK, commit(s)); // NOLINT
    EXPECT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing