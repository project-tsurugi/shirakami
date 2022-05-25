
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
        google::InitGoogleLogging("shirakami-test-concurrency_control-common-"
                                  "scan-c_scannable_total_index_size_test");
        FLAGS_stderrthreshold = 0;        // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(false, "/tmp/shirakami_c_scannable_total_index_size_test"); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(simple_scan, scannable_total_index_size_with_not_begin) { // NOLINT
    Storage st{};
    register_storage(st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::size_t sz{};
    ASSERT_EQ(Status::WARN_NOT_BEGIN,
              scannable_total_index_size(s, {}, sz));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
