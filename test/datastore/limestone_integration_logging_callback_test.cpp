
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "clock.h"
#include "test_tool.h"
#include "tsc.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/lpwal.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

#include "boost/filesystem.hpp"
#include "boost/foreach.hpp"

namespace shirakami::testing {

using namespace shirakami;

class limestone_integration_logging_callback_test
    : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-data_store-"
                "limestone_integration_logging_callback_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google; // NOLINT
};

TEST_F(limestone_integration_logging_callback_test, // NOLINT
       check_logging_callback) {                    // NOLINT
    // prepare test
    init({database_options::open_mode::CREATE}); // NOLINT
    database_set_logging_callback(
            [](std::size_t n, log_record* begin, log_record* end) {
                log_record* lrptr = begin;
                for (;;) {
                    LOG(INFO) << n << " " << lrptr->get_operation() << " "
                              << lrptr->get_key() << " " << lrptr->get_value()
                              << " " << lrptr->get_major_version() << " "
                              << lrptr->get_minor_version() << " "
                              << lrptr->get_storage_id();
                    if (lrptr == end) { break; }
                    ++lrptr;
                }
            });
    Storage st{};
    Storage st2{};
    ASSERT_EQ(Status::OK, create_storage(st));
    ASSERT_EQ(Status::OK, create_storage(st2));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // prepare data
    ASSERT_EQ(Status::OK, upsert(s, st, "a", "b"));
    ASSERT_EQ(Status::OK, upsert(s, st, "c", "d"));
    ASSERT_EQ(Status::OK, upsert(s, st2, "x", "y"));
    ASSERT_EQ(Status::OK, upsert(s, st2, "z", "w"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT // (*1)

    fin(false);
}

} // namespace shirakami::testing