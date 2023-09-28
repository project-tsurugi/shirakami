
#include <array>
#include <mutex>

#include "concurrency_control/include/epoch.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class database_options_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-start-database_options_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google;
};

TEST_F(database_options_test, print_out_object) { // NOLINT
    database_options options{};
    // check compile and running.
    LOG(INFO) << options;
    options.set_open_mode(database_options::open_mode::RESTORE);
    std::filesystem::path ldp{"hogehoge"};
    options.set_log_directory_path(ldp);
    LOG(INFO) << options;
    options.set_epoch_time(100); // NOLINT
    LOG(INFO) << options;

    // test about epoch time.
    init(options);
    ASSERT_EQ(epoch::get_global_epoch_time_us(), 100);
    fin();
    options.set_epoch_time(1);
    init(options);
    ASSERT_EQ(epoch::get_global_epoch_time_us(), 1);
    fin();
}

} // namespace shirakami::testing