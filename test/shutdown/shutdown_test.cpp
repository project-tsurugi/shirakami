
#include <mutex>
#include <memory>

#include "shirakami/interface.h"
#include "gtest/gtest.h"
#include "glog/logging.h"
#include "shirakami/api_diagnostic.h"
#include "shirakami/scheme.h"
#include "test_tool.h"

namespace shirakami::testing {

using namespace shirakami;

class shutdown_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-shut_down-shutdown_test_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google;
};

TEST_F(shutdown_test, fin) { // NOLINT
    ASSERT_EQ(init(), Status::OK);
    // meaningless fin
    fin();
    fin();
}

TEST_F(shutdown_test, without_free_session_resources) { // NOLINT
    ASSERT_OK(init());
    Token s{};
    Storage st{};
    ScanHandle sh{};
    ASSERT_OK(create_storage("a", st));
    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin(s));
    ASSERT_OK(insert(s, st, "k", "v"));
    ASSERT_OK(open_scan(s, st, {}, scan_endpoint::INF, {}, scan_endpoint::INF, sh));
    // without cleanup
    fin(); // LSAN may assert
}

} // namespace shirakami::testing
