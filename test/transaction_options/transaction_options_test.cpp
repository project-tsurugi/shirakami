
#include <array>
#include <mutex>

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class transaction_options_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-transaction_options-transaction_options_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google;
};

TEST_F(transaction_options_test, print_out_object) { // NOLINT
    transaction_options options{};
    // check compile and running.
    LOG(INFO) << options;
    options.set_token(&options);
    options.set_transaction_type(transaction_options::transaction_type::LONG);
    options.set_write_preserve({1, 2, 3});
    LOG(INFO) << options;
}

} // namespace shirakami::testing