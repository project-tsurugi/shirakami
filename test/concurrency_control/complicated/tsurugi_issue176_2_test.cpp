
#include <mutex>
#include <vector>

#include "clock.h"

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class tsurugi_issue176_2 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue176_2");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(tsurugi_issue176_2, comment_by_ban_20230226_0506) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    std::atomic<std::size_t> prepare_scan_thread_num{0};
    // fix these num to reinforce pressure
    // ==========
    constexpr std::size_t trial_num{5};
    constexpr std::size_t scan_thread_num{1};
    // ==========

    auto scan_worker = [st, &prepare_scan_thread_num]() {
        // prepare
        Token s{};
        ASSERT_EQ(Status::OK, enter(s));
        ++prepare_scan_thread_num;
        while (prepare_scan_thread_num.load(std::memory_order_acquire) !=
               scan_thread_num + 1) {
            _mm_pause();
        }
        // ready all thread

        // 100 trial
        for (std::size_t i = 0; i < trial_num; ++i) {
            // full scan, tx begin
            Status rc{};
            do {
                ASSERT_EQ(Status::OK,
                          tx_begin({
                                  s,
                                  transaction_options::transaction_type::LONG,
                          }));
                wait_epoch_update();
                // full scan
                ScanHandle hd{};
                rc = open_scan(s, st, "", scan_endpoint::INF, "",
                               scan_endpoint::INF, hd);
                if (rc == Status::OK) {
                    std::string buf{};
                    // read all cache
                    do {
                        read_key_from_scan(s, hd, buf);
                        rc = next(s, hd);
                    } while (rc != Status::WARN_SCAN_LIMIT);
                }

                // commit
                rc = commit(s);
                if (rc == Status::WARN_WAITING_FOR_OTHER_TX) {
                    do {
                        rc = check_commit(s);
                        _mm_pause();
                    } while (rc == Status::WARN_WAITING_FOR_OTHER_TX);
                }
            } while (rc != Status::OK);
        }

        // leave
        ASSERT_EQ(Status::OK, leave(s));
    };

    auto insert_delete_worker = [st, &prepare_scan_thread_num]() {
        // prepare
        Token s{};
        ASSERT_EQ(Status::OK, enter(s));
        ++prepare_scan_thread_num;
        while (prepare_scan_thread_num.load(std::memory_order_acquire) !=
               scan_thread_num + 1) {
            _mm_pause();
        }
        // ready all thread

        std::string key{"12345678"};
        std::string val{"v"};
        // 100 trial
        for (std::size_t i = 0; i < trial_num; ++i) {
            // tx: insert 100 records until commit
            Status rc{};
            do {
                ASSERT_EQ(Status::OK,
                          tx_begin({s,
                                    transaction_options::transaction_type::LONG,
                                    {st}}));
                wait_epoch_update();
                for (std::size_t j = 0; j < 100; ++j) { // NOLINT
                    memcpy(key.data(), &j, sizeof(j));
                    ASSERT_EQ(Status::OK, insert(s, st, key, val));
                }
                rc = commit(s);
                if (rc == Status::WARN_WAITING_FOR_OTHER_TX) {
                    do {
                        rc = check_commit(s);
                        _mm_pause();
                    } while (rc == Status::WARN_WAITING_FOR_OTHER_TX);
                }
            } while (rc != Status::OK);

            // tx: delete head 1 records until commit
            do {
                ASSERT_EQ(Status::OK,
                          tx_begin({s,
                                    transaction_options::transaction_type::LONG,
                                    {st}}));
                wait_epoch_update();
                std::size_t i_dummy = 0;
                memcpy(key.data(), &i_dummy, sizeof(i_dummy));
                ASSERT_EQ(Status::OK, delete_record(s, st, key));
                rc = commit(s);
                if (rc == Status::WARN_WAITING_FOR_OTHER_TX) {
                    do {
                        rc = check_commit(s);
                        _mm_pause();
                    } while (rc == Status::WARN_WAITING_FOR_OTHER_TX);
                }
            } while (rc != Status::OK);

            // tx: delete 99 records until commit
            do {
                ASSERT_EQ(Status::OK,
                          tx_begin({s,
                                    transaction_options::transaction_type::LONG,
                                    {st}}));
                wait_epoch_update();
                for (std::size_t j = 1; j < 100; ++j) { // NOLINT
                    memcpy(key.data(), &j, sizeof(j));
                    ASSERT_EQ(Status::OK, delete_record(s, st, key));
                }
                rc = commit(s);
                if (rc == Status::WARN_WAITING_FOR_OTHER_TX) {
                    do {
                        rc = check_commit(s);
                        _mm_pause();
                    } while (rc == Status::WARN_WAITING_FOR_OTHER_TX);
                }
            } while (rc != Status::OK);
        }

        // leave
        ASSERT_EQ(Status::OK, leave(s));
    };


    // test
    std::vector<std::thread> scan_thread_vc;
    scan_thread_vc.reserve(scan_thread_num);

    // start scan threads
    for (std::size_t i = 0; i < scan_thread_num; ++i) {
        scan_thread_vc.emplace_back(scan_worker);
    }

    std::thread insert_delete_thread = std::thread(insert_delete_worker);

    for (auto&& elem : scan_thread_vc) { elem.join(); }

    insert_delete_thread.join();
}

} // namespace shirakami::testing