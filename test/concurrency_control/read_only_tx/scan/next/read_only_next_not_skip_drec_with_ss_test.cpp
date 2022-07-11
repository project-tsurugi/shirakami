
#include <bitset>
#include <mutex>

#include "test_tool.h"

#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class read_only_next_not_skip_drec_with_ss_test
    : public ::testing::Test { // NOLINT

public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "scan-next_not_skip_drec_with_snap_test");
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

TEST_F(read_only_next_not_skip_drec_with_ss_test, // NOLINT
       next_not_skip_1_drec) {                    // NOLINT
    Storage st{};
    create_storage(st);
    Token s{};
    Token sl{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(sl));

    // prepare data
    std::string k1{"k1"};
    std::string k2{"k2"};
    std::string k3{"k3"};
    ASSERT_EQ(Status::OK, upsert(s, st, k1, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k2, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k3, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    wait_epoch_update();

    ASSERT_EQ(Status::OK, tx_begin({sl, transaction_options::transaction_type::READ_ONLY, {}}));
    wait_epoch_update();

    ASSERT_EQ(Status::OK, delete_record(s, st, k2));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(sl, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    // the cursor must point k1

    // test
    std::string sb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(sl, hd, sb));
    ASSERT_EQ(sb, k1);
    ASSERT_EQ(Status::OK, next(sl, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(sl, hd, sb));
    ASSERT_EQ(sb, k2);
    ASSERT_EQ(Status::OK, next(sl, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(sl, hd, sb));
    ASSERT_EQ(sb, k3);
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(sl, hd));
    ASSERT_EQ(Status::OK, commit(sl));

    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(sl));
}

TEST_F(read_only_next_not_skip_drec_with_ss_test, // NOLINT
       next_not_skip_2_drec) {                    // NOLINT
    Storage st{};
    create_storage(st);
    Token s{};
    Token sl{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(sl));

    // prepare data
    std::string k1{"k1"};
    std::string k2{"k2"};
    std::string k3{"k3"};
    std::string k4{"k4"};
    ASSERT_EQ(Status::OK, upsert(s, st, k1, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k2, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k3, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k4, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    wait_epoch_update();

    ASSERT_EQ(Status::OK, tx_begin({sl, transaction_options::transaction_type::READ_ONLY, {}}));
    wait_epoch_update();

    ASSERT_EQ(Status::OK, delete_record(s, st, k2));
    ASSERT_EQ(Status::OK, delete_record(s, st, k3));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(sl, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    // the cursor must point k1

    // test
    std::string sb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(sl, hd, sb));
    ASSERT_EQ(sb, k1);
    ASSERT_EQ(Status::OK, next(sl, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(sl, hd, sb));
    ASSERT_EQ(sb, k2);
    ASSERT_EQ(Status::OK, next(sl, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(sl, hd, sb));
    ASSERT_EQ(sb, k3);
    ASSERT_EQ(Status::OK, next(sl, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(sl, hd, sb));
    ASSERT_EQ(sb, k4);
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(sl, hd));
    ASSERT_EQ(Status::OK, commit(sl));

    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(sl));
}

TEST_F(read_only_next_not_skip_drec_with_ss_test, // NOLINT
       next_not_skip_3_drec) {                    // NOLINT
    Storage st{};
    create_storage(st);
    Token s{};
    Token sl{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(sl));

    // prepare data
    std::string k1{"k1"};
    std::string k2{"k2"};
    std::string k3{"k3"};
    std::string k4{"k4"};
    std::string k5{"k5"};
    ASSERT_EQ(Status::OK, upsert(s, st, k1, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k2, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k3, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k4, ""));
    ASSERT_EQ(Status::OK, upsert(s, st, k5, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    wait_epoch_update();

    ASSERT_EQ(Status::OK, tx_begin({sl, transaction_options::transaction_type::READ_ONLY, {}}));
    wait_epoch_update();

    ASSERT_EQ(Status::OK, delete_record(s, st, k2));
    ASSERT_EQ(Status::OK, delete_record(s, st, k3));
    ASSERT_EQ(Status::OK, delete_record(s, st, k4));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(sl, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    // the cursor must point k1

    // test
    std::string sb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(sl, hd, sb));
    ASSERT_EQ(sb, k1);
    ASSERT_EQ(Status::OK, next(sl, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(sl, hd, sb));
    ASSERT_EQ(sb, k2);
    ASSERT_EQ(Status::OK, next(sl, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(sl, hd, sb));
    ASSERT_EQ(sb, k3);
    ASSERT_EQ(Status::OK, next(sl, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(sl, hd, sb));
    ASSERT_EQ(sb, k4);
    ASSERT_EQ(Status::OK, next(sl, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(sl, hd, sb));
    ASSERT_EQ(sb, k5);
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(sl, hd));
    ASSERT_EQ(Status::OK, commit(sl));

    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(sl));
}

} // namespace shirakami::testing
