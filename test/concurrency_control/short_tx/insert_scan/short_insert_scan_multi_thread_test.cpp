
#include <xmmintrin.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <climits>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class short_insert_scan_multi_thread_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-short_tx-"
                "insert_scan-short_insert_scan_multi_thread_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_;
};

TEST_F(short_insert_scan_multi_thread_test, // NOLINT
       DISABLED_500_insert_and_full_scan) { // NOLINT
    // generate keys and table
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);

    struct S {
        static void scan_work(Storage st) {
            // prepare
            Token s{};
            ASSERT_EQ(enter(s), Status::OK);

            for (;;) {
                ScanHandle hd{};
                auto ret = open_scan(s, st, "", scan_endpoint::INF, "",
                                     scan_endpoint::INF, hd);
                if (ret != Status::OK) {
                    abort(s);
                    continue;
                }
                std::size_t ct{0};
                std::string buf{};
                do {
                    ret = read_key_from_scan(s, hd, buf);
                    if (ret == Status::OK) { ++ct; }
                    ret = next(s, hd);
                } while (ret == Status::OK);
                ret = commit(s); // NOLINT
                if (ret == Status::OK) {
                    ASSERT_EQ(ct % 500, 0);
                    if (ct == 5000) { break; } // NOLINT
                }
            }

            // cleanup
            ASSERT_EQ(leave(s), Status::OK);
        }

        static void insert_work(Storage st) {
            // prepare
            Token s{};
            ASSERT_EQ(enter(s), Status::OK);

            std::string k{"12345678"};
            for (std::size_t i = 0; i < 5000; ++i) { // NOLINT
                memcpy(k.data(), &i, sizeof(i));
                ASSERT_EQ(insert(s, st, k, "v"), Status::OK);
                if (i % 500 == 0) { // NOLINT
                    // commit each 500
                    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
                }
            }

            // cleanup
            ASSERT_EQ(leave(s), Status::OK);
        }
    };

    std::array<std::thread, 2> ths{};
    ths.at(0) = std::thread(S::insert_work, st);
    ths.at(1) = std::thread(S::scan_work, st);

    ths.at(0).join();
    ths.at(1).join();
}

} // namespace shirakami::testing