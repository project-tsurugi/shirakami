
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/ongoing_tx.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class delete_insert_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "interface-delete_insert-delete_insert_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google; // NOLINT
};

TEST_F(delete_insert_test, long_insert_execute_read) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));

    // data preparation
    ASSERT_EQ(insert(s1, st, "", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s1));
    // end preparation

    // test preparation
    ASSERT_EQ(Status::OK, tx_begin({s1, transaction_options::transaction_type::LONG, {st}}));
    ASSERT_EQ(Status::OK, tx_begin({s2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    // preparation

    // test
    /**
     * s1 failed insert and depends on existing the records.
     * Internally, s1 executed tx read operation for the records.
     */
    ASSERT_EQ(Status::WARN_ALREADY_EXISTS, insert(s1, st, "", ""));
    std::string buf{};
    // s2 forward for s1
    ASSERT_EQ(Status::OK, search_key(s2, st, "", buf));
    ASSERT_EQ(delete_record(s2, st, ""), Status::OK);

    ASSERT_EQ(Status::OK, commit(s1));
    /**
     * s1 is treated that it executed read operation.
     * Forwarded s2's delete_record will break s1, so it fails validation.
     */
    ASSERT_EQ(Status::ERR_VALIDATION, commit(s2));

    ASSERT_EQ(Status::OK, leave(s2));
    ASSERT_EQ(Status::OK, leave(s1));
}

} // namespace shirakami::testing