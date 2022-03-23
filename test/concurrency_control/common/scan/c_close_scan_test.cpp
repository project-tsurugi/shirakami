
#include <bitset>
#include <mutex>

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class simple_scan : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/build/test_log/close_scan_test_log");
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(false, log_dir_); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
    static inline std::string log_dir_; // NOLINT
};

TEST_F(simple_scan, close_scan_with_not_begin) { // NOLINT
    Storage st{};
    register_storage(st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::WARN_NOT_BEGIN, close_scan(s, {}));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_scan, close_scan) { // NOLINT
    Storage storage{};
    register_storage(storage);
    std::string k1("a"); // NOLINT
    std::string v1("0"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ScanHandle handle{};
    ASSERT_EQ(Status::OK, insert(s, storage, k1, v1));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, handle));
    ASSERT_EQ(Status::OK, close_scan(s, handle));
    ASSERT_EQ(Status::WARN_INVALID_HANDLE, close_scan(s, handle));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
