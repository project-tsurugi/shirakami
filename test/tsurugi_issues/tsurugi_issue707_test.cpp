
#include <atomic>
#include <functional>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "index/yakushima/include/interface.h"

#include "test_tool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

namespace shirakami::testing {

class tsurugi_issue707_test : public ::testing::TestWithParam<bool> {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue707_test");
        // FLAGS_stderrthreshold = 0;
        init_for_test();
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

static bool is_ready(const std::vector<char>& readys) {
    return std::all_of(readys.begin(), readys.end(),
                       [](char b) { return b != 0; });
}

static void wait_for_ready(const std::vector<char>& readys) {
    while (!is_ready(readys)) { _mm_pause(); }
}


void full_scan(Token t, Storage st, std::size_t const final_rec_num,
               std::size_t& count, Status& ret) {
    // init
    count = 0;

    ScanHandle shd{};
    Status rc = open_scan(t, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                          shd);
    ASSERT_TRUE(rc == Status::OK || rc == Status::WARN_NOT_FOUND ||
                rc == Status::ERR_CC);
    if (rc == Status::OK) {
        // loop and read all
        for (;;) {
            std::string buf{};
            rc = read_key_from_scan(t, shd, buf);
            if (rc == Status::OK) {
                ++count;
                if (count == final_rec_num) {
                    ret = Status::WARN_SCAN_LIMIT;
                    return;
                }
                rc = next(t, shd);
                if (rc == Status::WARN_SCAN_LIMIT) { break; }
                ASSERT_EQ(rc, Status::OK);
            } else {
                // abort and retry
                abort(t);
                ret = Status::ERR_CC;
                return;
            }
        }
        // check fin exp
        if (count == final_rec_num) {
            ret = Status::WARN_SCAN_LIMIT;
            return;
        }
        // check and return
        if (rc == Status::WARN_SCAN_LIMIT) {
            ret = Status::OK;
            return;
        }
    }
    // at least it can read ("0","0")
    LOG(FATAL) << rc;
    return;
}

INSTANTIATE_TEST_SUITE_P(is_insert, tsurugi_issue707_test,
                         ::testing::Values(true)); // success
// ::testing::Values(false)); // fail
// ::testing::Values(false, false)); // fail
// ::testing::Values(true, false)); // fail
// ::testing::Values(false, true)); // fail
// ::testing::Values(true, true)); // success

TEST_P(tsurugi_issue707_test, // NOLINT
       simple) {              // NOLINT
                              // prepare
    Storage st1{};
    Storage st2{};
    ASSERT_OK(create_storage("test1", st1));
    ASSERT_OK(create_storage("test2", st2));

    constexpr std::size_t th_num{60};
    constexpr std::size_t final_rec_num{25};
    constexpr std::size_t initial_rec_num{10};

    Token t{};
    ASSERT_OK(enter(t));
    ASSERT_OK(tx_begin({t, transaction_type::SHORT}));
    for (std::size_t i = 0; i < initial_rec_num; ++i) {
        ASSERT_OK(upsert(t, st1, std::to_string(i), std::to_string(i)));
    }
    ASSERT_OK(commit(t));

    // prepare write operator
    std::function<Status(Token, Storage, std::string_view, std::string_view)>
            write;
    if (GetParam()) {
        write = insert;
    } else {
        write = upsert;
    }

    std::vector<char> readys(th_num);
    std::atomic<bool> go{false};
    std::atomic<std::size_t> total_commit_ct{0};
    std::map<std::size_t, std::size_t> commit_map; // num, count;
    std::mutex mtx_commit_map;
    auto commit_map_add = [&commit_map, &mtx_commit_map](std::size_t i) {
        std::unique_lock<std::mutex> lk{mtx_commit_map};
        auto find_itr = commit_map.find(i);
        if (find_itr != commit_map.end()) { // hit
            find_itr->second += 1;
        } else {
            commit_map.insert(std::make_pair(i, 1));
        }
    };
    auto ltx_begin_wait = [](Token t) {
        TxStateHandle thd{};
        ASSERT_OK(acquire_tx_state_handle(t, thd));
        TxState ts{};
        do {
            ASSERT_OK(check_tx_state(thd, ts));
            std::this_thread::yield();
        } while (ts.state_kind() == TxState::StateKind::WAITING_START);
        ASSERT_OK(release_tx_state_handle(thd));
    };
    auto process = [st1, st2, final_rec_num, &readys, &go, &commit_map_add,
                    &total_commit_ct, write,
                    &ltx_begin_wait](std::size_t th_id) {
        std::size_t ct_abort{0};
        std::size_t ct_commit{0};
        // prepare
        Token t{};
        ASSERT_OK(enter(t));

        // ready to ready
        storeRelease(readys.at(th_id), 1);
        // ready
        while (!go.load(std::memory_order_acquire)) { _mm_pause(); }

        // exp

        for (;;) {
            // tx begin
            ASSERT_OK(tx_begin({t, transaction_type::LONG, {st1, st2}}));

            ltx_begin_wait(t);

            std::size_t count{0};

            // full scan st1
            Status rc{};
            ASSERT_NO_FATAL_FAILURE(
                    full_scan(t, st1, final_rec_num, count, rc));
            if (rc == Status::WARN_SCAN_LIMIT) {
                break;
            } else if (rc == Status::ERR_CC) {
                ++ct_abort;
                continue;
            }
            ASSERT_OK(rc);

            // write st1
            rc = write(t, st1, std::to_string(count + 1),
                       std::to_string(count + 1));
            ASSERT_TRUE(rc == Status::WARN_ALREADY_EXISTS || rc == Status::OK ||
                        rc == Status::ERR_CC);
            if (rc == Status::WARN_ALREADY_EXISTS) {
                abort(t);
                ++ct_abort;
                continue;
            }
            if (rc == Status::ERR_CC || rc == Status::WARN_ALREADY_EXISTS) {
                ++ct_abort;
                continue;
            }
            ASSERT_OK(rc);

            // verify for 1 write set element
            ASSERT_EQ(static_cast<session*>(t)
                              ->get_write_set()
                              .get_ref_cont_for_bt()
                              .size(),
                      1);

            // write st2
            rc = write(t, st2, std::to_string(count + 1),
                       std::to_string(count + 1));
            ASSERT_TRUE(rc == Status::WARN_ALREADY_EXISTS || rc == Status::OK ||
                        rc == Status::ERR_CC);
            if (rc == Status::WARN_ALREADY_EXISTS) {
                abort(t);
                ++ct_abort;
                continue;
            }
            if (rc == Status::ERR_CC || rc == Status::WARN_ALREADY_EXISTS) {
                ++ct_abort;
                continue;
            }
            ASSERT_OK(rc);

            // verify for 2 write set element
            ASSERT_EQ(static_cast<session*>(t)
                              ->get_write_set()
                              .get_ref_cont_for_bt()
                              .size(),
                      2);

            // commit
            rc = commit(t);

            // wait for result
            while (rc == Status::WARN_WAITING_FOR_OTHER_TX) {
                std::this_thread::yield();
                rc = check_commit(t);
            }

            if (rc == Status::OK) {
                ++ct_commit;
                commit_map_add(count + 1);
            } else {
                ++ct_abort;
            }
        }

        // cleanup
        ASSERT_OK(leave(t));
        LOG(INFO) << "thid " << th_id << ", commit " << ct_commit << ", abort "
                  << ct_abort;
        total_commit_ct += ct_commit;
    };

    // test
    std::vector<std::thread> thv{};
    thv.reserve(th_num);
    // stop gc
    {
        std::unique_lock lk{garbage::get_mtx_cleaner()};
        for (std::size_t i = 0; i < th_num; ++i) {
            thv.emplace_back(process, i);
        }

        wait_for_ready(readys);

        go.store(true, std::memory_order_release);

        for (auto&& th : thv) { th.join(); }
    }
    auto commit_map_verify = [&commit_map, final_rec_num, initial_rec_num]() {
        for (std::size_t i = 0; i < final_rec_num - initial_rec_num; ++i) {
            auto find_itr = commit_map.find(initial_rec_num + i + 1);
            ASSERT_NE(find_itr, commit_map.end()); // hit
            LOG(INFO) << "No. " << initial_rec_num + i + 1 << ", number "
                      << find_itr->second;
            ASSERT_EQ(find_itr->second, 1); // must 1
        }
    };

    ASSERT_OK(tx_begin({t, transaction_type::SHORT}));
    std::size_t count{0};
    Status rc{};
    // verify st1
    ASSERT_NO_FATAL_FAILURE(full_scan(t, st1, final_rec_num, count, rc));
    ASSERT_EQ(rc, Status::WARN_SCAN_LIMIT);
    ASSERT_EQ(count, final_rec_num);
    // verify st2
    ASSERT_NO_FATAL_FAILURE(full_scan(t, st2, final_rec_num, count, rc));
    ASSERT_EQ(rc, Status::OK);
    ASSERT_EQ(count, final_rec_num - initial_rec_num);
    ASSERT_OK(commit(t));
    // verify commit count
    // ASSERT_EQ(total_commit_ct, final_rec_num - initial_rec_num);
    LOG(INFO) << total_commit_ct << ", " << final_rec_num - initial_rec_num;
    // verify commit map
    ASSERT_NO_FATAL_FAILURE(commit_map_verify());

    // cleanup
    ASSERT_OK(leave(t));
}

} // namespace shirakami::testing