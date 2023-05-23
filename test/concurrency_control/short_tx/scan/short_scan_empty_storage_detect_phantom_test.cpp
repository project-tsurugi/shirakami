
#include <bitset>
#include <mutex>

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class short_scan_empty_storage_detect_phantom_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-short_tx-"
                "scan-short_scan_empty_storage_detect_phantom_test");
        FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(short_scan_empty_storage_detect_phantom_test, // NOLINT
       phantom_insert) {                             // NOLINT
                                                     // prepare
    Storage st{};
    create_storage("", st);
    std::string k("k"); // NOLINT
    std::string v("v"); // NOLINT
    Token s{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(s2));

    // test
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    std::vector<const Tuple*> records{};
    ScanHandle hd{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, open_scan(s, st, "", scan_endpoint::INF,
                                                "", scan_endpoint::INF, hd));
    // phantom insert
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s2, st, k, v));
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT
    // detect phantom
    ASSERT_EQ(Status::ERR_CC, commit(s)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing