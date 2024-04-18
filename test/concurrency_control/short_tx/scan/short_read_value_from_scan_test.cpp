
#include <mutex>
#include <string>

#include "shirakami/interface.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "shirakami/api_diagnostic.h"
#include "shirakami/api_storage.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"

namespace shirakami::testing {

using namespace shirakami;

class simple_scan : public ::testing::Test { // NOLINT

public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-common-"
                                  "scan-c_read_value_from_scan_test");
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

TEST_F(simple_scan, read_value_from_scan_with_not_begin) { // NOLINT
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::string sb{};
    ASSERT_EQ(Status::WARN_NOT_BEGIN, read_value_from_scan(s, {}, sb));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
