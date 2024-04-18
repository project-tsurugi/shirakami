
#include <mutex>
#include <memory>

#include "gtest/gtest.h"
#include "glog/logging.h"
#include "shirakami/api_diagnostic.h"
#include "shirakami/transaction_options.h"

namespace shirakami::testing {

using namespace shirakami;

class transaction_options_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-transaction_options-transaction_options_test");
        // FLAGS_stderrthreshold = 0;
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
    options.set_write_preserve({1, 2, 3}); // NOLINT
    // no read area
    options.set_read_area({});
    LOG(INFO) << options;
    // positive only
    options.set_read_area({{1}, {}}); // NOLINT
    LOG(INFO) << options;
    // negative only
    options.set_read_area({{}, {1}}); // NOLINT
    LOG(INFO) << options;
    // posi nega
    options.set_read_area({{1}, {2}}); // NOLINT
    LOG(INFO) << options;
    // multiple posi nega
    options.set_read_area({{1, 2}, {3, 4, 5}}); // NOLINT
    LOG(INFO) << options;
}

} // namespace shirakami::testing