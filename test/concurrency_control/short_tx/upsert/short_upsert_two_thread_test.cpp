
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
#include "concurrency_control/include/record.h"
#include "concurrency_control/include/version.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class short_upsert_two_thread_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-short_"
                                  "upsert_two_thread_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(short_upsert_two_thread_test,           // NOLINT
       10000_record_conflict_diff_direction) { // NOLINT
    /**
     * TxA write 0 to 10000
     * TxB write 10000 to 0
     * Commit same timing and if dead lock avoidance is correct, success.
     */
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    struct S {
        static void worker(Storage const st, std::size_t const th_id,
                           std::atomic<std::size_t>* const meet) {
            static constexpr std::size_t ary_size = 10000;
            Token s{};
            ASSERT_EQ(Status::OK, enter(s));
            std::string k{"12345678"};
            auto ret =
                    tx_begin({s, transaction_options::transaction_type::SHORT});
            if (ret != Status::OK) {
                LOG_FIRST_N(ERROR, 1)
                        << log_location_prefix << "unexpected error. " << ret;
            }
            for (std::size_t i = 0; i < ary_size; i++) {
                std::size_t buf{};
                if (th_id == 0) {
                    buf = i;
                } else if (th_id == 1) {
                    buf = ary_size - i - 1;
                } else {
                    LOG(FATAL);
                }
                std::memcpy(k.data(), &buf, sizeof(buf));
                ASSERT_EQ(upsert(s, st, k, "v"), Status::OK);
            }
            (*meet)++;
            while (meet->load(std::memory_order_acquire) != 2) { _mm_pause(); }
            // go same timing
            LOG(INFO) << th_id;
            ASSERT_EQ(commit(s), Status::OK);
            LOG(INFO) << th_id;
            ASSERT_EQ(Status::OK, leave(s));
        }
    };

    std::atomic<std::size_t> meet{0};
    std::thread a = std::thread(S::worker, st, 0, &meet);
    std::thread b = std::thread(S::worker, st, 1, &meet);
    a.join();
    b.join();
}

} // namespace shirakami::testing