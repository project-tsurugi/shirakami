
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/record.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "index/yakushima/include/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class long_delete_insert_scan_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "delete_insert-long_delete_insert_scan_test");
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

TEST_F(long_delete_insert_scan_test, scan_not_miss_converting_page) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));

    // create page x
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(insert(s1, st, "x", "v1"), Status::OK);
    ASSERT_EQ(Status::OK, commit(s1));

    // tx begin s1
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::LONG}));
    wait_epoch_update();
    // it can see x-v1

    // block gc by stopping epoch
    stop_epoch();

    auto work_s2 = [s2, st]() {
        // s2 delete x and insert x to create converting page
        ASSERT_EQ(Status::OK,
                  tx_begin({s2, transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(delete_record(s2, st, "x"), Status::OK);
        ASSERT_EQ(Status::OK, commit(s2));
        ASSERT_EQ(Status::OK,
                  tx_begin({s2, transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(insert(s2, st, "x", "v2"), Status::OK);
        ASSERT_EQ(Status::OK, commit(s2));
    };

    auto work_s1 = [s1, st]() {
        // test: s1 must see x-v1
        std::string buf{};
        ASSERT_EQ(Status::OK, search_key(s1, st, "x", buf));
        ASSERT_EQ(buf, "v1");
        ASSERT_EQ(Status::OK, commit(s1));
    };

    std::thread th_1 = std::thread(work_s1);
    std::thread th_2 = std::thread(work_s2);

    th_1.join();
    th_2.join();
    resume_epoch();

    // cleanup
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(delete_record(s1, st, "x"), Status::OK);
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing