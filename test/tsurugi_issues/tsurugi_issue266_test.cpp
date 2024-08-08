
#include <iomanip>
#include <sstream>

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"
#include "index/yakushima/include/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

// tsurugi issue #266: phantom when executing INSERT INTO tbl SELECT * FROM tbl, tbl contains 16 records, no PK 

namespace shirakami::testing {

class tsurugi_issue266_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-tsurugi_issues-tsurugi_issue266_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init();
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_;
};

static void do_test(unsigned int records) {
    // CREATE TABLE tbl (k INT)
    // INSERT INTO tbl VALUES (1)
    // repeat `records`-1 times more
    // INSERT INTO tbl SELECT * FROM tbl;

    Storage st;
    ASSERT_OK(create_storage("", st));
    wait_epoch_update();
    Token t;

    unsigned int next_key = 0;
    auto get_key_str = [](unsigned int i) {
        std::stringstream ss;
        ss << std::setw(8) << i;
        return ss.str();
    };
    auto get_next_key_str = [&next_key, &get_key_str] {
        return get_key_str(next_key++);
    };
    ASSERT_OK(enter(t));

    // setup
    ASSERT_OK(tx_begin(t));
    for (unsigned int i = 0; i < records; i++) {
        ASSERT_OK(insert(t, st, get_next_key_str(), ""));
    }
    ASSERT_OK(commit(t));

    LOG(INFO) << "start";

    ASSERT_OK(tx_begin(t));
    ScanHandle scanh;
    if (auto rc = open_scan(t, st, "", scan_endpoint::INF, "", scan_endpoint::INF, scanh); rc != Status::OK) {
        LOG(ERROR) << "open_scan rc:" << rc;
    } else {
        do {
            std::string key;
            std::string val;
            auto rc = read_key_from_scan(t, scanh, key);
            if (rc == Status::WARN_NOT_FOUND) { continue; }
            if (rc == Status::WARN_CONCURRENT_INSERT) { continue; }
            ASSERT_OK(rc);
            VLOG(10) << "read_key result: <" << key << ">";
            rc = read_value_from_scan(t, scanh, val);
            if (rc != Status::OK) {
                VLOG(10) << "read_value_from_scan rc:" << rc;
                continue;
            }
            std::string nextkey{get_next_key_str()};
            ASSERT_OK(insert(t, st, nextkey, ""));
            VLOG(10) << "inserted: <" << nextkey << ">";
        } while (next(t, scanh) == Status::OK);
        ASSERT_OK(close_scan(t, scanh));
    }
    ASSERT_OK(commit(t));

    LOG(INFO) << "end";

    ASSERT_OK(leave(t));
}

TEST_F(tsurugi_issue266_test, len8) {
    do_test(8);
}

TEST_F(tsurugi_issue266_test, len15) {
    do_test(15);
}

TEST_F(tsurugi_issue266_test, len16) {
    do_test(16);
}

} // namespace shirakami::testing
