
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

class tsurugi_issue665_test : public ::testing::TestWithParam<bool> {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue665_test");
        FLAGS_stderrthreshold = 0;
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

INSTANTIATE_TEST_SUITE_P(is_insert, tsurugi_issue665_test,
                         ::testing::Values(true, false));

TEST_F(tsurugi_issue665_test, // NOLINT
       DISABLED_stall_test) { // NOLINT
    // comment for https://github.com/project-tsurugi/tsurugi-issues/issues/665#issuecomment-2000185872
    /**
     * test senario
     * t0: insert a, commit to make test node
     * t1: full scan, insert b
     * t2: insert b (sharing), abort
     * sleep
     * gc: unhook b
     * t3: insert c (must occur phantom for t1 node verify)
     * stop gc
     * t1: commit, node verify try phis insert and find phantom, lock release
     * for locked records (but it miss the insert with lock).
     * t4: insert and share it.
     * t4: commit it and wait lock infinite
    */
    // prepare
    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token t1{};
    Token t2{};
    Token t3{};
    Token t4{};
    ASSERT_OK(enter(t1));
    ASSERT_OK(enter(t2));
    ASSERT_OK(enter(t3));
    ASSERT_OK(enter(t4));
    // test
    // t0: insert a, commit to make test node
    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));
    ASSERT_OK(upsert(t1, st, "a", ""));
    ASSERT_OK(commit(t1));
    // t1
    ASSERT_OK(tx_begin({t1, transaction_type::SHORT}));
    // full scan
    ScanHandle shd{};
    ASSERT_OK(open_scan(t1, st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                        shd));
    std::string buf{};
    ASSERT_OK(read_key_from_scan(t1, shd, buf));
    ASSERT_EQ(buf, "a");
    ASSERT_EQ(next(t1, shd), Status::WARN_SCAN_LIMIT);
    // insert
    ASSERT_OK(insert(t1, st, "b", ""));
    // t2
    ASSERT_OK(tx_begin({t2, transaction_type::SHORT}));
    ASSERT_OK(insert(t2, st, "b", "")); // sharing
    ASSERT_OK(abort(t2));
    // sleep for unhook
    Status rc{};
    do {
        std::this_thread::yield();
        Record* rec_ptr{};
        rc = get<Record>(st, "b", rec_ptr);
    } while (rc == Status::OK);
    // t3
    ASSERT_OK(tx_begin({t3, transaction_type::SHORT}));
    ASSERT_OK(insert(t3, st, "c", "")); // sharing
    // stop gc
    {
        std::unique_lock lk{garbage::get_mtx_cleaner()};
        ASSERT_EQ(commit(t1), Status::ERR_CC); // craete locked missed record b
        // t4
        ASSERT_OK(tx_begin({t4, transaction_type::SHORT}));
        ASSERT_OK(insert(t4, st, "b", "")); // sharing b
        ASSERT_OK(commit(t4));              // stall
    }

    // cleanup
    ASSERT_OK(leave(t1));
    ASSERT_OK(leave(t2));
    ASSERT_OK(leave(t3));
    ASSERT_OK(leave(t4));
}

TEST_P(tsurugi_issue665_test, // NOLINT
       simple) {              // NOLINT
                              // prepare
    Storage st1{};
    Storage st2{};
    ASSERT_OK(create_storage("test1", st1));
    ASSERT_OK(create_storage("test2", st2));

    constexpr std::size_t th_num{60};
    constexpr std::size_t final_rec_num{300};
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
    auto process = [st1, st2, final_rec_num, &readys, &go, &commit_map_add,
                    &total_commit_ct, write](std::size_t th_id) {
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
            ASSERT_OK(tx_begin({t, transaction_type::SHORT}));

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

            // write st1, st2
            rc = write(t, st1, std::to_string(count + 1),
                       std::to_string(count + 1));
            ASSERT_TRUE(rc == Status::WARN_ALREADY_EXISTS || rc == Status::OK ||
                        rc == Status::ERR_CC);
            if (rc == Status::WARN_ALREADY_EXISTS) { abort(t); }
            if (rc == Status::ERR_CC || rc == Status::WARN_ALREADY_EXISTS) {
                ++ct_abort;
                continue;
            }
            ASSERT_OK(rc);
            rc = write(t, st2, std::to_string(count + 1),
                       std::to_string(count + 1));
            ASSERT_TRUE(rc == Status::WARN_ALREADY_EXISTS || rc == Status::OK ||
                        rc == Status::ERR_CC);
            if (rc == Status::WARN_ALREADY_EXISTS) { abort(t); }
            if (rc == Status::ERR_CC || rc == Status::WARN_ALREADY_EXISTS) {
                ++ct_abort;
                continue;
            }
            ASSERT_OK(rc);

            // commit
            rc = commit(t);
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
    for (std::size_t i = 0; i < th_num; ++i) { thv.emplace_back(process, i); }

    wait_for_ready(readys);

    go.store(true, std::memory_order_release);

    for (auto&& th : thv) { th.join(); }

    auto commit_map_verify = [&commit_map, final_rec_num, initial_rec_num]() {
        for (std::size_t i = 0; i < final_rec_num - initial_rec_num; ++i) {
            auto find_itr = commit_map.find(initial_rec_num + i + 1);
            ASSERT_NE(find_itr, commit_map.end()); // hit
            LOG(INFO) << initial_rec_num + i + 1  << ", " << find_itr->second;
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