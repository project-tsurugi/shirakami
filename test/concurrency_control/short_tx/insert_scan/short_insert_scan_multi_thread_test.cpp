
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

// library

#include "shirakami/interface.h"

#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

// test tool

#include "test_tool.h"

// google
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
        // FLAGS_stderrthreshold = 0;
        init_for_test();
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
       500_insert_and_full_scan) {          // NOLINT
    // generate keys and table
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);

    constexpr std::size_t scan_th_num{1};
    constexpr std::size_t delta{500};
    constexpr std::size_t rec_num{5000};

    struct S {
        static void scan_work(Storage st, std::size_t const rec_num,
                              std::size_t const delta) {
            // prepare
            Token s{};
            ASSERT_EQ(enter(s), Status::OK);
            for (;;) {
                ScanHandle hd{};
                ASSERT_EQ(Status::OK,
                          tx_begin({s, transaction_options::transaction_type::
                                               SHORT}));
                auto ret = open_scan(s, st, "", scan_endpoint::INF, "",
                                     scan_endpoint::INF, hd);
                if (ret != Status::OK) {
                    if (ret == Status::ERR_CC) { continue; }
                    ASSERT_EQ(Status::OK, abort(s));
                    continue;
                }
                std::size_t ct = 0;
                std::string buf{};
                do {
                    ret = read_key_from_scan(s, hd, buf);
                    if (ret == Status::OK) {
                        ++ct;
                    } else if (ret == Status::ERR_CC) {
                        break;
                    }
                    ret = next(s, hd);
                } while (ret == Status::OK);
                if (ret == Status::ERR_CC) { continue; }
                ret = commit(s); // NOLINT
                if (ret == Status::OK) {
                    ASSERT_EQ(ct % delta, 0);
                    if (ct == rec_num) { break; } // NOLINT
                }
            }

            // cleanup
            ASSERT_EQ(leave(s), Status::OK);
        }

        static void insert_work(Storage st, std::size_t const rec_num,
                                std::size_t const delta) {
            // prepare
            Token s{};
            ASSERT_EQ(enter(s), Status::OK);

            std::string k{"12345678"}; // to allocate 8 bites definitely.
            for (std::size_t i = 1; i <= rec_num; ++i) { // NOLINT
                auto* ti = static_cast<session*>(s);
                if (!ti->get_tx_began()) {
                    ASSERT_EQ(Status::OK,
                              tx_begin({s, transaction_options::
                                                   transaction_type::SHORT}));
                }
                memcpy(k.data(), &i, sizeof(i));
                ASSERT_EQ(insert(s, st, k, "v"), Status::OK);
                if (i % delta == 0) { // NOLINT
                    // commit each 500
                    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
                    // if this is enable, you can see commit each 500 record.
                    // sleep(1);
                }
            }
            ASSERT_EQ(Status::WARN_NOT_BEGIN, commit(s)); // NOLINT

            // cleanup
            ASSERT_EQ(leave(s), Status::OK);
        }
    };

    std::vector<std::thread> scan_ths{};
    for (std::size_t i = 0; i < scan_th_num; ++i) {
        scan_ths.emplace_back(std::thread(S::scan_work, st, rec_num, delta));
    }
    std::thread insert_th = std::thread(S::insert_work, st, rec_num, delta);

    insert_th.join();
    for (auto&& th : scan_ths) { th.join(); }
}

} // namespace shirakami::testing