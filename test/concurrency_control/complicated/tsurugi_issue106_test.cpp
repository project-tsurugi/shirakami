
#include <glog/logging.h>

#include <mutex>

#include "clock.h"

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class tsurugi_issue106 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue106");
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

/**
 * Issue 106 caused by single thread.
 */

TEST_F(tsurugi_issue106, simple1) { // NOLINT
    /**
     * Tx1. delete all. scan and delete but the workload is known as no record
     * exist.
     * 
     * Tx2. Insert some key twice. The second see already exist and do user abort.
     * 
     * Tx3. full scan and commit.
     */
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("test", st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    for (std::size_t i = 0; i < 500; ++i) {
        // Tx1.
        ScanHandle hd{};
        ASSERT_EQ(Status::WARN_NOT_FOUND,
                  open_scan(s, st, "", scan_endpoint::INF, "",
                            scan_endpoint::INF, hd));
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT

        // Tx2.
        for (std::size_t i = 0; i < 100; ++i) {
            std::string k(1, i);
            ASSERT_EQ(Status::OK, insert(s, st, k, "v"));
        }

        // second
        for (std::size_t i = 0; i < 100; ++i) {
            std::string k(1, i);
            ASSERT_EQ(Status::WARN_ALREADY_EXISTS, insert(s, st, k, "v"));
        }

        // user abort
        ASSERT_EQ(Status::OK, abort(s));

        // Tx3. full scan
        // record empty state
        ASSERT_EQ(Status::WARN_NOT_FOUND,
                  open_scan(s, st, "", scan_endpoint::INF, "",
                            scan_endpoint::INF, hd));
        // but node set is not empty
        auto* ti = static_cast<session*>(s);
        ASSERT_EQ(ti->get_node_set().empty(), false);
        // expect Status::OK
        ASSERT_EQ(Status::OK, commit(s));
    }

    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing