
#include <atomic>
#include <functional>
#include <xmmintrin.h>

#include "shirakami/interface.h"
#include "test_tool.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class tsurugi_issue438_2_test : public ::testing::TestWithParam<bool> {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue438_2");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_;
};

INSTANTIATE_TEST_SUITE_P(revorder, tsurugi_issue438_2_test,
                         ::testing::Values(false, true));

void scan_and_read(Token& t, Storage& st, std::string_view lk, scan_endpoint le,
                   std::string_view rk, scan_endpoint re) {
    ScanHandle sh;
    auto rc = open_scan(t, st, lk, le, rk, re, sh);
    if (rc == Status::OK) {
        do {
            std::string buf{};
            ASSERT_OK(read_key_from_scan(t, sh, buf));
        } while (next(t, sh) == Status::OK);
        ASSERT_OK(close_scan(t, sh));
    } else if (rc == Status::WARN_NOT_FOUND) {
        // nop
    } else {
        LOG(FATAL) << "open_scan rc:" << rc;
    }
}

void full_scan(Token& t, Storage& st) {
    scan_and_read(t, st, "", scan_endpoint::INF, "", scan_endpoint::INF);
}

TEST_F(tsurugi_issue438_2_test, case_1) {
    LOG(INFO) << "case 1";
    Storage st;
    ASSERT_OK(create_storage("", st));
    wait_epoch_update();

    Token t1;
    Token t2;

    // [TX1]
    // begin long transaction wp tb1;
    // select * from tb1;
    // insert into tb1 values (2, 100)
    LOG(INFO) << "TX1 begin";
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {st}}));
    wait_epoch_update();
    LOG(INFO) << "TX1 select full";
    full_scan(t1, st);
    LOG(INFO) << "TX1 insert 2";
    ASSERT_OK(insert(t1, st, "2", "100"));
    wait_epoch_update();

    // [TX2]
    // begin long transaction wp tb1;
    // select * from tb1;
    // insert into tb1 values (3, 100)
    LOG(INFO) << "TX2 begin";
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t2, transaction_type::LONG, {st}}));
    wait_epoch_update();
    LOG(INFO) << "TX2 select full";
    full_scan(t2, st);
    LOG(INFO) << "TX2 insert 3";
    ASSERT_OK(insert(t2, st, "3", "100"));
    wait_epoch_update();

    // [TX1]
    // Commit;
    LOG(INFO) << "TX1 Commit";
    ASSERT_OK(commit(t1));
    ASSERT_OK(leave(t1));
    // [TX2]
    // Commit;→コミットできてしまう。
    LOG(INFO) << "TX2 Commit (must fail)";
    ASSERT_EQ(commit(t2), Status::ERR_CC);
    ASSERT_OK(leave(t2));

    ASSERT_OK(delete_storage(st));
}

TEST_P(tsurugi_issue438_2_test, case_2) {
    bool rev = GetParam();
    LOG(INFO) << "case 2b" << std::boolalpha << rev;
    Storage st;
    ASSERT_OK(create_storage("", st));
    wait_epoch_update();

    Token t1;
    Token t2;
    // TX1の側のreadをfull scanでなく、point readにしてみたが、同じでバグる
    // 初期：空テーブル

    // TX1 begin LTX:WP=tb1
    LOG(INFO) << "TX1 begin";
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {st}}));
    wait_epoch_update();
    // Select * from tb1 where id=1;
    LOG(INFO) << "TX1 Select id=1";
    std::string val;
    ASSERT_EQ(search_key(t1, st, "1", val), Status::WARN_NOT_FOUND);
    // Insert into tb1 values(6,600)
    LOG(INFO) << "TX1 Insert 6";
    ASSERT_OK(insert(t1, st, "6", "600"));
    wait_epoch_update();

    // Tx2 begin LTX:WP=tb1
    LOG(INFO) << "TX2 begin";
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t2, transaction_type::LONG, {st}}));
    wait_epoch_update();
    // Select * from tb1 ;
    LOG(INFO) << "TX2 Select full";
    full_scan(t2, st);
    // Insert into tb1 values(1,100); //updateから修正→起きることも確認済み
    LOG(INFO) << "TX2 Insert 1";
    ASSERT_OK(insert(t2, st, "1", "100"));
    wait_epoch_update();

    if (!rev) {
        // Tx1::Commit
        LOG(INFO) << "TX1 Commit";
        ASSERT_OK(commit(t1));
        ASSERT_OK(leave(t1));

        // Tx2::Commit→通ってしまう。
        LOG(INFO) << "TX2 Commit (must fail)";
        ASSERT_EQ(commit(t2), Status::ERR_CC);
        ASSERT_OK(leave(t2));
    } else {
        // 逆順
        std::atomic_bool t2c_done = false;
        std::atomic<Status> t2c_rc;

        LOG(INFO) << "TX2 Commit (wait)";
        ASSERT_FALSE(commit(
                t2, [&t2c_done,
                     &t2c_rc](Status rc, [[maybe_unused]] reason_code reason,
                              [[maybe_unused]] durability_marker_type dm) {
                    t2c_rc = rc;
                    t2c_done = true;
                }));

        // Tx1::Commit
        LOG(INFO) << "TX1 Commit";
        ASSERT_OK(commit(t1));
        ASSERT_OK(leave(t1));

        LOG(INFO) << "TX2 ... resume Commit (fail)";
        while (!t2c_done) { _mm_pause(); }
        ASSERT_EQ(t2c_rc.load(), Status::ERR_CC);
        ASSERT_OK(leave(t2));
    }
    ASSERT_OK(delete_storage(st));
}

TEST_P(tsurugi_issue438_2_test, case_3) {
    bool rev = GetParam();
    LOG(INFO) << "case 3b" << std::boolalpha << rev;
    Storage st;
    ASSERT_OK(create_storage("", st));
    wait_epoch_update();

    Token t1;
    Token t2;
    // Rangeもアウトっぽい
    // TX1側をpoint readにして、TX2側のscanをrangeにしてみたが同じくbugっている

    // 初期データ
    // [1,100]
    // [2,200]
    // [3,300]
    // [4,400]
    // [5,500]
    // [6,600]
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin(t1));
    ASSERT_OK(insert(t1, st, "1", "100"));
    ASSERT_OK(insert(t1, st, "2", "200"));
    ASSERT_OK(insert(t1, st, "3", "300"));
    ASSERT_OK(insert(t1, st, "4", "400"));
    ASSERT_OK(insert(t1, st, "5", "500"));
    ASSERT_OK(insert(t1, st, "6", "600"));
    ASSERT_OK(commit(t1));
    ASSERT_OK(leave(t1));
    wait_epoch_update();

    // TX1 begin LTX:WP=tb1
    LOG(INFO) << "TX1 begin";
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {st}}));
    wait_epoch_update();
    // Select * from tb1 where id=1; //point read
    LOG(INFO) << "TX1 Select 1";
    std::string val;
    ASSERT_OK(search_key(t1, st, "1", val));
    // Insert into tb1 values(7,700)
    LOG(INFO) << "TX1 Insert 7";
    ASSERT_OK(insert(t1, st, "7", "700"));
    wait_epoch_update();

    // Tx2 begin LTX:WP=tb1
    LOG(INFO) << "TX2 begin";
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t2, transaction_type::LONG, {st}}));
    wait_epoch_update();
    // Select * from tb1 where id >=2
    // (key=2,3,4,5,6が返る。これはOK）
    LOG(INFO) << "TX2 Select 2-INF";
    scan_and_read(t2, st, "2", scan_endpoint::INCLUSIVE, "",
                  scan_endpoint::INF);
    // Update tb1 set value=150 where id=1;
    LOG(INFO) << "TX2 Update 1";
    ASSERT_OK(update(t2, st, "1", "150"));
    wait_epoch_update();

    if (!rev) {
        // Tx1::Commit
        LOG(INFO) << "TX1 Commit";
        ASSERT_OK(commit(t1));
        ASSERT_OK(leave(t1));
        wait_epoch_update();

        // Tx2::Commit→通ってしまう。(1,150)にセットされている。
        LOG(INFO) << "TX2 Commit (must fail)";
        ASSERT_EQ(commit(t2), Status::ERR_CC);
        ASSERT_OK(leave(t2));
        wait_epoch_update();
    } else {
        // 逆順
        std::atomic_bool t2c_done = false;
        std::atomic<Status> t2c_rc;

        LOG(INFO) << "TX2 Commit (wait)";
        ASSERT_EQ(commit(t2,
                         [&t2c_done, &t2c_rc](
                                 Status rc, [[maybe_unused]] reason_code reason,
                                 [[maybe_unused]] durability_marker_type dm) {
                             t2c_rc = rc;
                             t2c_done = true;
                         }),
                  false);

        // Tx1::Commit
        LOG(INFO) << "TX1 Commit";
        ASSERT_OK(commit(t1));
        ASSERT_OK(leave(t1));

        LOG(INFO) << "TX2 ... resume Commit (fail)";
        while (!t2c_done) { _mm_pause(); }
        ASSERT_EQ(t2c_rc.load(), Status::ERR_CC);
        ASSERT_OK(leave(t2));
    }

    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin(t1));
    ASSERT_OK(search_key(t1, st, "1", val));
    ASSERT_EQ(val, "100");
    ASSERT_OK(commit(t1));
    ASSERT_OK(leave(t1));

    ASSERT_OK(delete_storage(st));
}

TEST_P(tsurugi_issue438_2_test, case_d2) {
    bool wait_after_delete = GetParam();
    LOG(INFO) << "case 2 after delete";
    Storage st;

    ASSERT_OK(create_storage("", st));
    wait_epoch_update();

    Token t1;
    Token t2;
    // TID-000000000000001a > BEGIN;
    // TID-000000000000001a > insert into tb1 values(1,100);
    // TID-000000000000001a > COMMIT;
    // TID-000000000000001b > BEGIN;
    // TID-000000000000001b > delete from tb1;
    // TID-000000000000001b > COMMIT;

    LOG(INFO) << "TX8: begin occ";
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin(t1));
    LOG(INFO) << "TX8: insert 1";
    ASSERT_OK(insert(t1, st, "1", "100"));
    LOG(INFO) << "TX8: commit";
    ASSERT_OK(commit(t1));
    ASSERT_OK(leave(t1));
    wait_epoch_update();
    wait_epoch_update();

    LOG(INFO) << "TX9: begin occ";
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin(t2));
    LOG(INFO) << "TX9: delete 1";
    ASSERT_OK(delete_record(t2, st, "1"));
    LOG(INFO) << "TX9: commit";
    ASSERT_OK(commit(t2));
    ASSERT_OK(leave(t2));
    if (wait_after_delete) {
        LOG(INFO) << "wait one epoch";
        wait_epoch_update(); // may wait gc end
    }

    // setup done

    // TID-000000000000001c > BEGIN LONG TRANSACTION WRITE PRESERVE tb1;
    // TID-000000000000001c > Select * from tb1 where id=1;
    // TID-000000000000001c >>> results: []
    // TID-000000000000001c > Insert into tb1 values(6,600);
    LOG(INFO) << "TX1: begin ltx wp";
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin({t1, transaction_type::LONG, {st}}));
    wait_epoch_update();
    LOG(INFO) << "TX1: select 1";
    std::string val;
    ASSERT_EQ(search_key(t1, st, "1", val), Status::WARN_NOT_FOUND);
    LOG(INFO) << "TX1: insert 6";
    ASSERT_OK(insert(t1, st, "6", "600"));
    wait_epoch_update();

    // TID-0000000100000001 > BEGIN LONG TRANSACTION WRITE PRESERVE tb1;
    // TID-0000000100000001 > Select * from tb1;
    // TID-0000000100000001 >>> results: []
    // TID-0000000100000001 > Insert into tb1 values(1,200);
    LOG(INFO) << "TX2: begin ltx wp";
    ASSERT_OK(enter(t2));
    ASSERT_OK(tx_begin({t2, transaction_type::LONG, {st}}));
    wait_epoch_update();
    LOG(INFO) << "TX2: select full";
    full_scan(t2, st);
    LOG(INFO) << "TX2: insert 1";
    ASSERT_OK(insert(t2, st, "1", "100"));
    wait_epoch_update();

    // TID-000000000000001c > COMMIT;
    // TID-0000000100000001 > COMMIT;
    LOG(INFO) << "TX1 Commit";
    ASSERT_OK(commit(t1));
    ASSERT_OK(leave(t1));
    LOG(INFO) << "TX2 Commit (must fail)";
    ASSERT_EQ(commit(t2), Status::ERR_CC);
    ASSERT_OK(leave(t2));

    ASSERT_OK(delete_storage(st));
}

} // namespace shirakami::testing
