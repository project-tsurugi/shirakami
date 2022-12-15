
#include <xmmintrin.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::testing {

class search_update : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-silo-search_update_test");
        FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

/**
 * test list
 * pointReadUpdate
 * repeatableReadUpdateDiffPayloadSizeByMt
 */

TEST_F(search_update, pointReadUpdate) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    std::string k("k");   // NOLINT
    std::string v("v");   // NOLINT
    std::string v2("v2"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(upsert(s, st, k, v), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s, st, k, vb));
    ASSERT_EQ(Status::OK, update(s, st, k, v2));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
}

TEST_F(search_update, // NOLINT
       repeatableReadUpdateDiffPayloadSizeByMt) {
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    std::string k("k");       // NOLINT
    std::string v1(8, 'v');   // NOLINT
    std::string v2(100, 'v'); // NOLINT

    // prepare data
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(upsert(s, st, k, v1), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));

    std::atomic<std::size_t> ready{0};
    std::atomic<std::size_t> work_a_cnt{0};
    std::atomic<std::size_t> work_b_cnt{0};
    std::mutex mtx_ready;
    std::condition_variable cond;
    auto work = [st, k, &ready, &mtx_ready, &cond, &work_a_cnt,
                 &work_b_cnt](std::string const& v, bool is_a) {
        Token s{};
        ASSERT_EQ(Status::OK, enter(s));

        ++ready;
        // wait for other
        {
            std::unique_lock<std::mutex> lk{mtx_ready};

            cond.wait(lk, [&ready] { return ready == 2; });
        }
        if (ready != 2) { LOG(ERROR); }

        LOG(INFO) << "start work";

        for (;;) {
            std::string vb{};
            auto rc{search_key(s, st, k, vb)};
            for (;;) {
                if (rc == Status::OK) { break; }
                if (rc == Status::WARN_CONCURRENT_UPDATE) {
                    rc = search_key(s, st, k, vb);
                } else {
                    LOG(ERROR);
                }
            }
            ASSERT_EQ(Status::OK, update(s, st, k, v));
            rc = commit(s); // NOLINT
            if (rc == Status::OK) {
                if (is_a) {
                    ++work_a_cnt;
                } else {
                    ++work_b_cnt;
                }
            }
            if (work_a_cnt > 10 && work_b_cnt > 10) { break; } // NOLINT
        }
        ASSERT_EQ(Status::OK, leave(s));
    };

    std::thread work_a(work, v1, true);
    std::thread work_b(work, v2, false);

    // ready
    for (;;) {
        if (ready == 2) {
            // go
            cond.notify_all();
            break;
        }
        _mm_pause();
    }

    work_a.join();
    work_b.join();

    LOG(INFO) << "work_a_cnt:\t" << work_a_cnt;
    LOG(INFO) << "work_b_cnt:\t" << work_b_cnt;
}

} // namespace shirakami::testing