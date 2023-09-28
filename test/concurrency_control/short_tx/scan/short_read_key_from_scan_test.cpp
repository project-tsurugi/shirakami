
#include <mutex>

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class simple_scan : public ::testing::Test { // NOLINT

public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-common-"
                                  "scan-c_read_key_from_scan_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(simple_scan, read_key_from_scan_with_not_begin) { // NOLINT
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::string sb{};
    ASSERT_EQ(Status::WARN_NOT_BEGIN, read_key_from_scan(s, {}, sb));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_scan, test_after_delete_api) { // NOLINT
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    {
        stop_epoch();
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
        ASSERT_EQ(Status::OK, commit(s));
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, delete_record(s, st, ""));
        ASSERT_EQ(Status::OK, commit(s));
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::SHORT}));
        ScanHandle hd{};
        ASSERT_EQ(Status::WARN_NOT_FOUND,
                  open_scan(s, st, "", scan_endpoint::INF, "",
                            scan_endpoint::INF, hd));
    }
    resume_epoch();
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
