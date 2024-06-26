
#include <cmath>

#include <mutex>

#include "concurrency_control/include/ongoing_tx.h"

#include "storage.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::testing {

class storage_get_set_options_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-storage-storage_get_set_options_test");
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

TEST_F(storage_get_set_options_test, storage_get_set_options_test) { // NOLINT
    Storage st{};
    storage_option options{};
    // storage not found
    ASSERT_EQ(Status::WARN_NOT_FOUND, storage_get_options(st, options));
    ASSERT_EQ(Status::WARN_NOT_FOUND, storage_set_options(st, options));
    ASSERT_EQ(Status::OK, create_storage("1", st, {2, "3"}));
    // storage found
    ASSERT_EQ(Status::OK, storage_get_options(st, options));
    ASSERT_EQ(options.id(), 2);
    ASSERT_EQ(options.payload(), "3");
    options.id(4);
    options.payload("5");
    ASSERT_EQ(Status::OK, storage_set_options(st, options));
    // reset
    options.id(0);
    options.payload("");
    ASSERT_EQ(Status::OK, storage_get_options(st, options));
    // check set options effect
    ASSERT_EQ(options.id(), 4);
    ASSERT_EQ(options.payload(), "5");
    ASSERT_EQ(Status::OK, delete_storage(st));
    // storage not found
    ASSERT_EQ(Status::WARN_NOT_FOUND, storage_get_options(st, options));
    ASSERT_EQ(Status::WARN_NOT_FOUND, storage_set_options(st, options));
}

} // namespace shirakami::testing
