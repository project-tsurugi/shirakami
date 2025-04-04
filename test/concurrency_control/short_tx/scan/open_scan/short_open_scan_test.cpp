
#include <mutex>

#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "glog/logging.h"

#include "gtest/gtest.h"

#include "test_tool.h"

namespace shirakami::testing {

using namespace shirakami;

class open_scan_test : public ::testing::Test { // NOLINT

public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-common-"
                                  "scan-c_open_scan_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(open_scan_test, max_size_test) { // NOLINT
    Storage storage{};
    create_storage("", storage);
    std::string k1("k1"); // NOLINT
    std::string k2("k2"); // NOLINT
    std::string k3("k3"); // NOLINT
    std::string v1("v1"); // NOLINT
    std::string v2("v2"); // NOLINT
    std::string v3("v3"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    // parepare data
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, storage, k1, v1));
    ASSERT_EQ(Status::OK, insert(s, storage, k2, v1));
    ASSERT_EQ(Status::OK, insert(s, storage, k3, v1));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // open scan
    ScanHandle handle{};
    std::size_t len{};
    {
        ASSERT_EQ(Status::OK,
                  tx_begin({s, transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, handle));
        ASSERT_OK(scannable_total_index_size(s, handle, len));
        ASSERT_EQ(len, 3);
    }
    {
        ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, handle, 1));
        ASSERT_OK(scannable_total_index_size(s, handle, len));
        ASSERT_EQ(len, 1);
    }
    {
        ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, handle, 2));
        ASSERT_OK(scannable_total_index_size(s, handle, len));
        ASSERT_EQ(len, 2);
    }
    {
        ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, handle, 3));
        ASSERT_OK(scannable_total_index_size(s, handle, len));
        ASSERT_EQ(len, 3);
    }
    {
        ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                        scan_endpoint::INF, handle, 4));
        ASSERT_OK(scannable_total_index_size(s, handle, len));
        ASSERT_EQ(len, 3);
    }
}

TEST_F(open_scan_test, multi_open) { // NOLINT
    Storage storage{};
    create_storage("", storage);
    std::string k1("a"); // NOLINT
    std::string v1("0"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ScanHandle handle{};
    ScanHandle handle2{};
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              open_scan(s, storage, "", scan_endpoint::INF, "",
                        scan_endpoint::INF, handle));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, storage, k1, v1));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, handle));
    ASSERT_NE(nullptr, handle);
    ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, handle2));
    ASSERT_NE(nullptr, handle2);
    ASSERT_NE(handle, handle2);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(open_scan_test, multi_open_reading_values) { // NOLINT
    Storage storage{};
    create_storage("", storage);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ScanHandle handle{};
    ScanHandle handle2{};
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              open_scan(s, storage, "", scan_endpoint::INF, "",
                        scan_endpoint::INF, handle));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, storage, "a/1", "1"));
    ASSERT_EQ(Status::OK, insert(s, storage, "b/3", "3"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, open_scan(s, storage, "a/", scan_endpoint::INCLUSIVE,
                                    "", scan_endpoint::INF, handle));
    ASSERT_NE(nullptr, handle);

    std::string sb{};
    ASSERT_EQ(Status::OK, read_value_from_scan(s, handle, sb));
    ASSERT_EQ("1", sb);

    ASSERT_EQ(Status::OK, open_scan(s, storage, "b/", scan_endpoint::INCLUSIVE,
                                    "", scan_endpoint::INF, handle2));
    ASSERT_NE(nullptr, handle2);
    ASSERT_NE(handle, handle2);
    ASSERT_EQ(Status::OK, read_value_from_scan(s, handle2, sb));
    ASSERT_EQ("3", sb);

    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(open_scan_test, empty_range) {
    Storage storage{};
    create_storage("", storage);
    Token s{};
    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(insert(s, storage, "a", "val"));
    ScanHandle handle{};
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              open_scan(s, storage,
                        "b", scan_endpoint::INCLUSIVE,
                        "a", scan_endpoint::INCLUSIVE, handle));
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              open_scan(s, storage,
                        "a", scan_endpoint::INCLUSIVE,
                        "a", scan_endpoint::EXCLUSIVE, handle));
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              open_scan(s, storage,
                        "a", scan_endpoint::EXCLUSIVE,
                        "a", scan_endpoint::EXCLUSIVE, handle));
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              open_scan(s, storage,
                        "a", scan_endpoint::EXCLUSIVE,
                        "a", scan_endpoint::INCLUSIVE, handle));
    ASSERT_OK(commit(s));
    ASSERT_OK(leave(s));
}

} // namespace shirakami::testing
