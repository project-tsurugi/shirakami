
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <memory>
#include <mutex>

#include "shirakami/interface.h"
#include "test_tool.h"
#include "gtest/gtest.h"
#include "shirakami/api_diagnostic.h"
#include "shirakami/database_options.h"
#include "shirakami/transaction_options.h"


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;



namespace shirakami::testing {

class maintenance_mode_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-datastore-maintenance_mode_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(maintenance_mode_test, removed) { // NOLINT
    database_options options{};
    options.set_open_mode(database_options::open_mode::MAINTENANCE);
    ASSERT_EQ(init(options), Status::ERR_INVALID_CONFIGURATION);
}

} // namespace shirakami::testing
