
#include <mutex>
#include <cstddef>
#include <memory>
#include <ostream>

#include "concurrency_control/bg_work/include/bg_commit.h"
#include "shirakami/interface.h"
#include "gtest/gtest.h"
#include "glog/logging.h"
#include "shirakami/database_options.h"
#include "shirakami/scheme.h"

namespace shirakami::testing {

using namespace shirakami;

class bg_commit_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-bg_work-bg_commit_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google;
};

TEST_F(bg_commit_test, print_out_object) { // NOLINT
    database_options options{};
    LOG(INFO) << "before set" << options;
    constexpr std::size_t wrtnum{64};
    options.set_waiting_resolver_threads(wrtnum);
    LOG(INFO) << "after set" << options;

    LOG(INFO) << "before init "
              << bg_work::bg_commit::joined_waiting_resolver_threads();
    init(options);
    ASSERT_EQ(bg_work::bg_commit::joined_waiting_resolver_threads(), 0);
    fin();
    ASSERT_EQ(bg_work::bg_commit::joined_waiting_resolver_threads(), wrtnum);
}

} // namespace shirakami::testing