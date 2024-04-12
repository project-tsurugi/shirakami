
#include "clock.h"

#include "test_tool.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class shirakami_issue141 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-shirakami_issue141");
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

// 1. Storage = ["a", "c"]
// 2. OCC1: full-scan, upsert "b"
// 3. OCC2: full-scan, upsert "b"
// 4. OCC2: commit
// 5. OCC1: commit
// 6. either commit must fail

TEST_F(shirakami_issue141, DISABLED_not_serializable_2occ) {
    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s1{};
    Token s2{};
    std::string buf{};

    // prepare record
    ASSERT_OK(enter(s1));
    ASSERT_OK(enter(s2));
    ASSERT_OK(tx_begin(s1));
    ASSERT_OK(upsert(s1, st, "a", "s0"));
    ASSERT_OK(upsert(s1, st, "c", "s0"));
    ASSERT_OK(commit(s1));
    // wait epoch change
    wait_epoch_update();

    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    // OCC1: full scan (reads "a", "c")
    ScanHandle shd1{};
    ASSERT_OK(open_scan(s1, st, "", scan_endpoint::INF, "",
                        scan_endpoint::INF, shd1));
    ASSERT_OK(read_key_from_scan(s1, shd1, buf));
    ASSERT_EQ(buf, "a");
    ASSERT_OK(next(s1, shd1));
    ASSERT_OK(read_key_from_scan(s1, shd1, buf));
    ASSERT_EQ(buf, "c");
    ASSERT_EQ(next(s1, shd1), Status::WARN_SCAN_LIMIT);
    ASSERT_OK(close_scan(s1, shd1));
    // OCC1: upsert
    ASSERT_OK(upsert(s1, st, "b", "s1"));

    // OCC2: full scan (reads "a", "c")
    ASSERT_OK(tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ScanHandle shd2{};
    ASSERT_OK(open_scan(s2, st, "", scan_endpoint::INF, "",
                        scan_endpoint::INF, shd2));
    ASSERT_OK(read_key_from_scan(s2, shd2, buf));
    ASSERT_EQ(buf, "a");
    ASSERT_OK(next(s2, shd2));
    auto rc_s2readkey = read_key_from_scan(s2, shd2, buf);
    if (rc_s2readkey == Status::WARN_CONCURRENT_INSERT) {
        ASSERT_OK(next(s2, shd2)); // skip inserting <b,s1>
        rc_s2readkey = read_key_from_scan(s2, shd2, buf);
    }
    ASSERT_OK(rc_s2readkey);
    ASSERT_EQ(buf, "c");
    ASSERT_EQ(next(s2, shd2), Status::WARN_SCAN_LIMIT);
    ASSERT_OK(close_scan(s2, shd2));
    // OCC2: upsert
    ASSERT_OK(upsert(s2, st, "b", "s2"));

    // OCC2: commit
    auto rc_s2commit = commit(s2);

    // OCC1: commit
    auto rc_s1commit = commit(s1);

    // OCC1 or OCC2 must fail
    ASSERT_FALSE(rc_s1commit == Status::OK && rc_s2commit == Status::OK);

    // cleanup
    ASSERT_OK(leave(s1));
    ASSERT_OK(leave(s2));
}

} // namespace shirakami::testing
