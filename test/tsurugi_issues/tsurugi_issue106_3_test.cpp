
#include <mutex>

#include "clock.h"

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;



namespace shirakami::testing {

class tsurugi_issue106_3 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue106_3");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }
    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

// for issue#106
// fail commit by ERR_CC, shirakami::node_verify detects version change
// at i = 63
//    node_version64_body{locked:0 inserting_deleting:0 splitting:0 deleted:0 root:1 border:1 vinsert_delete:3 vsplit:0}
// -> node_version64_body{locked:0 inserting_deleting:0 splitting:0 deleted:0 root:1 border:1 vinsert_delete:4 vsplit:0}
// and at i = 7
//    node_version64_body{locked:0 inserting_deleting:0 splitting:0 deleted:0 root:0 border:1 vinsert_delete:16 vsplit:1}
// -> node_version64_body{locked:0 inserting_deleting:0 splitting:0 deleted:0 root:1 border:1 vinsert_delete:16 vsplit:1}

std::string mk_key(int i) {
    std::stringstream x;
    x << std::setw(11) << i; // NOLINT
    return x.str();
}

TEST_F(tsurugi_issue106_3, 20230319_comment_ban) { // NOLINT
    auto pred_wait = [](int i) {
        return (i == 7 || i == 8 || i == 63 || i == 64); // NOLINT
    };

    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s{};

    // setup nodes [1, records]
    ASSERT_OK(enter(s));
    int records = 136; // needs >= 136 // NOLINT
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    for (int i = 1; i <= records; i++) {
        ASSERT_OK(insert(s, st, mk_key(i), ""));
    }
    ASSERT_OK(commit(s));
    ASSERT_OK(leave(s));

    LOG(INFO) << "all vinsert_delete must not be changed after this.";

    // repeat deleting records from tail
    for (int i = records; i > 0; i--) { // NOLINT
        ScanHandle scan{};
        ASSERT_OK(enter(s));
        ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
        ASSERT_OK(open_scan(s, st, mk_key(i), scan_endpoint::INCLUSIVE,
                            mk_key(i + 1), scan_endpoint::EXCLUSIVE, scan));
        if (pred_wait(i)) {
            sleepUs(100 * 1000); // may run GC // NOLINT
        }
        std::string key{};
        ASSERT_OK(read_key_from_scan(s, scan, key));
        ASSERT_OK(delete_record(s, st, key));
        ASSERT_EQ(next(s, scan), Status::WARN_SCAN_LIMIT);
        ASSERT_OK(close_scan(s, scan));
        ASSERT_OK(commit(s));
        ASSERT_OK(leave(s));
    }
}

} // namespace shirakami::testing