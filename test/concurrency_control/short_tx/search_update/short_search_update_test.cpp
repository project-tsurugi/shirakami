
#include <emmintrin.h>
#include <atomic>
#include <mutex>
#include <thread>
#include <cstddef>
#include <ostream>
#include <string>

#include "shirakami/interface.h"
#include "gtest/gtest.h"
#include "glog/logging.h"
#include "shirakami/api_diagnostic.h"
#include "shirakami/api_storage.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"
#include "shirakami/transaction_options.h"

using namespace shirakami;

namespace shirakami::testing {

class search_update : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-silo-search_update_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
        // FLAGS_logbuflevel = -1; // no buffering
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
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(upsert(s, st, k, v), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
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
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(upsert(s, st, k, v1), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));

    std::atomic<std::size_t> ready{0};
    std::atomic<std::size_t> work_a_cnt{0};
    std::atomic<std::size_t> work_b_cnt{0};
    auto work = [st, k, &ready, &work_a_cnt, &work_b_cnt](std::string const& v, bool is_a) {
        Token s{};
        ASSERT_EQ(Status::OK, enter(s));

        ++ready;
        // wait for other
        while (ready.load() != 2) { _mm_pause(); }

        LOG(INFO) << "start work";
        std::size_t loop1_cnt{0};

        for (;;) {
            ASSERT_EQ(Status::OK,
                      tx_begin({s,
                                transaction_options::transaction_type::SHORT}));
            std::string vb{};
            auto rc{search_key(s, st, k, vb)};
            std::size_t loop2_cnt{0};
            for (;;) {
                // search must return ok or warn not found.
                if (rc == Status::OK || rc == Status::WARN_CONCURRENT_UPDATE) {
                    break;
                }
                if (rc == Status::WARN_NOT_FOUND) {
                    // why not found?  does retry make sense?
                    rc = search_key(s, st, k, vb);
                } else {
                    GTEST_FAIL() << "rc: " << rc;
                }
                ++loop2_cnt;
                ASSERT_LT(loop2_cnt, 1000) << "loop2: " << loop2_cnt << ", rc: " << rc;
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
            ++loop1_cnt;
            ASSERT_LT(loop1_cnt, 1000) << "loop1: " << loop1_cnt << ", rc: " << rc;
        }
        ASSERT_EQ(Status::OK, leave(s));
    };

    std::thread work_a(work, v1, true);
    std::thread work_b(work, v2, false);

    work_a.join();
    work_b.join();

    LOG(INFO) << "work_a_cnt:\t" << work_a_cnt;
    LOG(INFO) << "work_b_cnt:\t" << work_b_cnt;
}

} // namespace shirakami::testing
