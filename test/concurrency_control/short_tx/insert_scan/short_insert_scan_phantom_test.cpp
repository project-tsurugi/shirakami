
#include <mutex>
#include <thread>

#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "test_tool.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::testing {

class insert_scan_phantom_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-short_tx-"
                                  "insert_scan-insert_scan_phantom_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(insert_scan_phantom_test, scan_insert_tx_find_phantom) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("test", st));
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(upsert(s1, st, "1", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    // test
    // s1 scan and register node info
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ScanHandle handle{};
    ASSERT_EQ(Status::OK, open_scan(s1, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, handle));
    std::string sb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s1, handle, sb));
    ASSERT_EQ(sb, "1");
    // s2 insert and commit
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(upsert(s2, st, "2", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT
    // s1 insert 3 and find phantom
    ASSERT_EQ(insert(s1, st, "3", ""), Status::ERR_CC);
    auto* ti = static_cast<session*>(s1);
    ASSERT_TRUE(ti->get_result_info().get_has_key_info());
    ASSERT_TRUE(ti->get_result_info().get_has_storage_name_info());
    LOG(INFO) << ti->get_result_info();

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(insert_scan_phantom_test, scan_find_phantom_by_insert) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("test", st));
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(upsert(s1, st, "1", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s1)); // NOLINT

    // test
    // s1 scan and register node info
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ScanHandle handle{};
    ASSERT_EQ(Status::OK, open_scan(s1, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, handle));
    std::string sb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s1, handle, sb));
    ASSERT_EQ(sb, "1");
    // s2 insert and commit
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(insert(s2, st, "2", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT
    // s1 commit and find phantom
    ASSERT_EQ(Status::ERR_CC, commit(s1)); // NOLINT
    auto* ti = static_cast<session*>(s1);
    ASSERT_FALSE(ti->get_result_info().get_has_key_info());
    ASSERT_FALSE(ti->get_result_info().get_has_storage_name_info());
    LOG(INFO) << ti->get_result_info();

    // cleanup
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

// issue 1543
void scan_find_phantom_by_insert_deep_sub(std::string_view s2_key) {
    // insert operations may create new border nodes.
    // these nodes must be tracked by node set for phantom check.

    // expect
    // s1: begin                       -> OK
    // s2: begin                       -> OK
    // s1: full scan                   -> OK
    // s1: insert 1111111122222222a    -> OK
    // s1: insert 1111111122222222b    -> OK (ensure that new layer is created for suffix-enabled yakushima)
    // s2: insert 11111111AAA          -> OK (insert to newly created L1 border node)
    //  OR insert 1111111122222222AAA  -> OK (insert to newly created L2 border node)
    // s2: commit                      -> OK
    // s1: commit                      -> ERR_CC by node verify (PHANTOM)

    // prepare
    Storage st{};
    ASSERT_OK(create_storage("test", st));
    Token s1{};
    Token s2{};
    ASSERT_OK(enter(s1));
    ASSERT_OK(enter(s2));
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s1, st, "1", ""));
    ASSERT_OK(commit(s1));

    // test
    // s1 scan and register node info
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ScanHandle handle{};
    ASSERT_OK(open_scan(s1, st, {}, scan_endpoint::INF, {}, scan_endpoint::INF, handle));
    std::string sb{};
    ASSERT_OK(read_key_from_scan(s1, handle, sb));
    ASSERT_EQ(sb, "1");
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s1, handle));
    ASSERT_OK(close_scan(s1, handle));
    // s1 insert long key
    ASSERT_OK(insert(s1, st, "1111111122222222a", "")); // suffix-enabled yakushima does not create new layer
    ASSERT_OK(insert(s1, st, "1111111122222222b", "")); // until second long-key put operation
    // s2 insert and commit
    ASSERT_OK(tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(insert(s2, st, s2_key, ""));
    ASSERT_EQ(Status::OK, commit(s2));
    // s1 commit and find phantom
    ASSERT_EQ(Status::ERR_CC, commit(s1)) << "s2_key:" << s2_key;

    // cleanup
    ASSERT_OK(leave(s1));
    ASSERT_OK(leave(s2));
}

// issue 1543
TEST_F(insert_scan_phantom_test, DISABLED_scan_find_phantom_by_insert_deep_l1) {
    scan_find_phantom_by_insert_deep_sub("11111111AAA");
}

// issue 1543
TEST_F(insert_scan_phantom_test, DISABLED_scan_find_phantom_by_insert_deep_l2) {
    scan_find_phantom_by_insert_deep_sub("1111111122222222AAA");
}

} // namespace shirakami::testing
