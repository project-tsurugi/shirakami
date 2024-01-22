
#include <mutex>
#include <vector>

#include "clock.h"

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class tsurugi_issue361 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue361");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(tsurugi_issue361, occ_must_not_read_uncommitted_record) { // NOLINT
    auto count = [](Storage& st) {
        std::size_t num{0};
        Token s2{};
        LOG_IF(FATAL, enter(s2) != Status::OK);
        LOG_IF(FATAL, tx_begin({s2}) != Status::OK);
        ScanHandle sh{};
        auto rc = open_scan(s2, st, "", scan_endpoint::INF, "",
                            scan_endpoint::INF, sh);
        if (rc == Status::OK) {
            do {
                std::string buf{};
                rc = read_key_from_scan(s2, sh, buf);
                LOG_IF(FATAL, rc != Status::OK &&
                                      rc != Status::WARN_CONCURRENT_INSERT);
                LOG(INFO) << rc << ", " << buf;
                ++num;
            } while (next(s2, sh) == Status::OK);
        } else if (rc == Status::WARN_NOT_FOUND) {
            num = 0;
        } else {
            LOG(FATAL) << "open_scan rc:" << rc;
        }
        LOG_IF(FATAL, commit(s2) != Status::OK);
        LOG_IF(FATAL, leave(s2) != Status::OK);
        return num;
    };

    Storage st{};
    ASSERT_OK(create_storage("", st));

    Token s{};

    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin({s}));
    ASSERT_OK(insert(s, st, "1", ""));

    /**
     * occ reads "1" as cc protocl but doesn't read as service, so return code
     * was Status::WARN_CONCURRENT_INSERT.
    */
    ASSERT_EQ(count(st), 1);

    ASSERT_OK(commit(s)); // 0 -> 1
    ASSERT_OK(leave(s));

    /**
     * occ reads "1" as cc protocl and service. (Status::OK)
    */
    ASSERT_EQ(count(st), 1);

    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin({s}));
    ASSERT_OK(insert(s, st, "2", ""));

    /**
     * occ reads "1" as cc protocl and service. (Status::OK)
     * occ reads "2" as cc protocl but doesn't read as service, so return code
     * was Status::WARN_CONCURRENT_INSERT.
    */
    ASSERT_EQ(count(st), 2);

    ASSERT_OK(commit(s)); // 1 -> 2
    ASSERT_OK(leave(s));

    /**
     * occ reads "1" and "2" as cc protocl and service. (Status::OK)
    */
    ASSERT_EQ(count(st), 2);
}

} // namespace shirakami::testing