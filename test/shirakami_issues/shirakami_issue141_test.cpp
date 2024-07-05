
#include "clock.h"

#include "test_tool.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

enum step {
    occ1_full_scan, occ2_full_scan,
    occ1_upsert_b, occ2_upsert_b,
    occ1_commit, occ2_commit,

    // for extra scenarios
    occ1_insert_b, occ2_insert_b,
    occ1_searchkey, occ2_searchkey,
};

struct scenario {
    scenario(std::string desc, std::vector<step> steps) : desc(desc), steps(steps) {}

    std::string desc;
    std::vector<step> steps;
};

class shirakami_issue141 : public ::testing::TestWithParam<scenario> { // NOLINT
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

TEST_F(shirakami_issue141, not_serializable_2occ) {
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

// similar scenarios (for regression test)

INSTANTIATE_TEST_SUITE_P(scinarios, shirakami_issue141, ::testing::Values(
    // scenario{ "original_case",
    //     { occ1_full_scan, occ1_upsert_b, occ2_full_scan, occ2_upsert_b, occ2_commit, occ1_commit }},
    scenario{ "occ2scan_comes_first",
        { occ1_full_scan, occ2_full_scan, occ1_upsert_b, occ2_upsert_b, occ2_commit, occ1_commit }},
    scenario{ "occ1commit_comes_first",
        { occ1_full_scan, occ1_upsert_b, occ2_full_scan, occ2_upsert_b, occ1_commit, occ2_commit }},
    scenario{ "searchkey",
        { occ1_searchkey, occ1_upsert_b, occ2_searchkey, occ2_upsert_b, occ2_commit, occ1_commit }},
    scenario{ "insert",
        { occ1_full_scan, occ1_insert_b, occ2_full_scan, occ2_insert_b, occ2_commit, occ1_commit }}
    ), [](const ::testing::TestParamInfo<scenario>& info){ return info.param.desc; } );

TEST_P(shirakami_issue141, not_serializable_2occ_steps) {
    Storage st{};
    Token s1{};
    Token s2{};

    auto full_scan_must_reads_ac = [&st](Token& s) {
        std::string buf{};
        ScanHandle shd{};
        ASSERT_OK(open_scan(s, st, "", scan_endpoint::INF, "", scan_endpoint::INF, shd));
        ASSERT_OK(read_key_from_scan(s, shd, buf));
        ASSERT_EQ(buf, "a");
        ASSERT_OK(next(s, shd));
        auto rc_readkey = read_key_from_scan(s, shd, buf);
        if (rc_readkey == Status::WARN_CONCURRENT_INSERT) {
            ASSERT_OK(next(s, shd)); // skip inserting <b,s1>
            rc_readkey = read_key_from_scan(s, shd, buf);
        }
        ASSERT_OK(rc_readkey);
        ASSERT_EQ(buf, "c");
        ASSERT_EQ(next(s, shd), Status::WARN_SCAN_LIMIT);
        ASSERT_OK(close_scan(s, shd));
    };
    auto search_key_must_not_read_b = [&st](Token& s) {
        std::string buf{};
        auto rc_searchkey = search_key(s, st, "b", buf);
        ASSERT_TRUE(rc_searchkey == Status::WARN_NOT_FOUND || rc_searchkey == Status::WARN_CONCURRENT_INSERT);
    };

    ASSERT_OK(create_storage("", st));
    ASSERT_OK(enter(s1));
    ASSERT_OK(enter(s2));

    // prepare record
    ASSERT_OK(tx_begin(s1));
    ASSERT_OK(upsert(s1, st, "a", "s0"));
    ASSERT_OK(upsert(s1, st, "c", "s0"));
    ASSERT_OK(commit(s1));
    // wait epoch change
    wait_epoch_update();

    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(tx_begin({s2, transaction_options::transaction_type::SHORT}));

    Status rc_s1commit{Status::ERR_FATAL};
    Status rc_s2commit{Status::ERR_FATAL};

    auto scenario = GetParam();
    for (auto step : scenario.steps) {
        switch (step) {
            case occ1_full_scan: full_scan_must_reads_ac(s1); break;
            case occ2_full_scan: full_scan_must_reads_ac(s2); break;
            case occ1_searchkey: search_key_must_not_read_b(s1); break;
            case occ2_searchkey: search_key_must_not_read_b(s2); break;
            case occ1_upsert_b:
                ASSERT_OK(upsert(s1, st, "b", "s1"));
                break;
            case occ2_upsert_b:
                ASSERT_OK(upsert(s2, st, "b", "s2"));
                break;
            case occ1_insert_b:
                ASSERT_OK(insert(s1, st, "b", "s1"));
                break;
            case occ2_insert_b:
                ASSERT_OK(insert(s2, st, "b", "s2"));
                break;
            case occ1_commit: rc_s1commit = commit(s1); break;
            case occ2_commit: rc_s2commit = commit(s2); break;
        }
    }
    ASSERT_NE(rc_s1commit, Status::ERR_FATAL);
    ASSERT_NE(rc_s2commit, Status::ERR_FATAL);

    // either OCC1 or OCC2 must fail
    ASSERT_FALSE(rc_s1commit == Status::OK && rc_s2commit == Status::OK);
    // either OCC1 or OCC2 must success
    ASSERT_TRUE(rc_s1commit == Status::OK || rc_s2commit == Status::OK);

    // cleanup
    ASSERT_OK(leave(s1));
    ASSERT_OK(leave(s2));
}

} // namespace shirakami::testing
