
#include <mutex>
#include <cstddef>
#include <memory>

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
                                  "scan-c_scannable_total_index_size_test");
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

TEST_F(simple_scan, scannable_total_index_size_with_not_begin) { // NOLINT
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::size_t sz{};
    ASSERT_EQ(Status::WARN_NOT_BEGIN, scannable_total_index_size(s, {}, sz));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
