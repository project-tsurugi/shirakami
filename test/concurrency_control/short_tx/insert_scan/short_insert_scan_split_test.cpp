
#include "test_tool.h"

#include "shirakami/interface.h"
#include "index/yakushima/include/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class short_insert_scan_split_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-short_tx-insert_scan-"
                                  "short_insert_scan_split_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init();
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_;
};

void full_scan(Token& t, Storage& st) {
    ScanHandle sh;
    auto rc = open_scan(t, st, "", scan_endpoint::INF, "", scan_endpoint::INF, sh);
    if (rc == Status::OK) {
        while (next(t, sh) == Status::OK) {
            std::string buf{};
            ASSERT_OK(read_key_from_scan(t, sh, buf));
        }
        ASSERT_OK(close_scan(t, sh));
    } else if (rc == Status::WARN_NOT_FOUND) {
        // nop
    } else {
        LOG(FATAL) << "open_scan rc:" << rc;
    }
}

TEST_F(short_insert_scan_split_test, ok_singletx_mtroot) {
    // no phantom if split by single tx
    // split position masstree root border

    // setup
    // storage: AA, BA, CA, DA, EA, FA, GA, HA, IA, JA, KA, LA, MA, NA, OA (15 keys)

    // expect
    // s1: begin                       -> OK
    // s1: full scan                   -> OK
    // s1: insert AB                   -> OK
    // s1: commit                      -> OK

    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s1{};
    ASSERT_OK(enter(s1));

    // setup
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    for (char c = 'A'; c <= 'O'; c++) {
        char buf[2] = { c, 'A' };
        ASSERT_OK(upsert(s1, st, std::string_view(buf, 2U), "0")) << c;
    }
    ASSERT_OK(commit(s1));

    // test
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));

    wait_epoch_update();

    full_scan(s1, st);
    ASSERT_OK(insert(s1, st, "AB", "1"));
    ASSERT_OK(commit(s1));

    ASSERT_OK(leave(s1));
}

TEST_F(short_insert_scan_split_test, phantom_multitx_mtroot_left) {
    // phantom if another tx inserts into left split node
    // split position: masstree root border

    // setup
    // storage: AA, BA, CA, DA, EA, FA, GA, HA, IA, JA, KA, LA, MA, NA, OA (15 keys)

    // expect
    // s1: begin                       -> OK
    // s2: begin                       -> OK
    // s1: full scan                   -> OK
    // s1: insert AB                   -> OK
    // s2: insert BB                   -> OK
    // s2: commit                      -> OK
    // s1: commit                      -> ERR_CC by node verify (PHANTOM)

    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_OK(enter(s1));
    ASSERT_OK(enter(s2));

    // setup
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    for (char c = 'A'; c <= 'O'; c++) {
        char buf[2] = { c, 'A' };
        ASSERT_OK(upsert(s1, st, std::string_view(buf, 2U), "0")) << c;
    }
    ASSERT_OK(commit(s1));

    // test
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(tx_begin({s2, transaction_options::transaction_type::SHORT}));

    wait_epoch_update();

    full_scan(s1, st);
    ASSERT_OK(insert(s1, st, "AB", "1")); // split
    ASSERT_OK(insert(s2, st, "BB", "2")); // into left
    ASSERT_OK(commit(s2));
    ASSERT_EQ(commit(s1), Status::ERR_CC);

    ASSERT_OK(leave(s1));
    ASSERT_OK(leave(s2));
}

// issue 1453
TEST_F(short_insert_scan_split_test, phantom_multitx_mtroot_right) {
    // phantom if another tx inserts into right split node
    // split position: masstree root border

    // setup
    // storage: AA, BA, CA, DA, EA, FA, GA, HA, IA, JA, KA, LA, MA, NA, OA (15 keys)

    // expect
    // s1: begin                       -> OK
    // s2: begin                       -> OK
    // s1: full scan                   -> OK
    // s1: insert AB                   -> OK
    // s2: insert NB                   -> OK
    // s2: commit                      -> OK
    // s1: commit                      -> ERR_CC by node verify (PHANTOM)

    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_OK(enter(s1));
    ASSERT_OK(enter(s2));

    // setup
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    for (char c = 'A'; c <= 'O'; c++) {
        char buf[2] = { c, 'A' };
        ASSERT_OK(upsert(s1, st, std::string_view(buf, 2U), "0")) << c;
    }
    ASSERT_OK(commit(s1));

    // test
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(tx_begin({s2, transaction_options::transaction_type::SHORT}));

    wait_epoch_update();

    full_scan(s1, st);
    ASSERT_OK(insert(s1, st, "AB", "1")); // split
    ASSERT_OK(insert(s2, st, "NB", "2")); // into right
    ASSERT_OK(commit(s2));
    ASSERT_EQ(commit(s1), Status::ERR_CC);

    ASSERT_OK(leave(s1));
    ASSERT_OK(leave(s2));
}

// issue 1453
TEST_F(short_insert_scan_split_test, phantom_multitx_nonroot_right) {
    // phantom if another tx inserts into right split node
    // split position: non-masstree root border (parent is interior node)

    // setup
    // insert 00, 01, 02, 03, 04, 05, 06, 07, AA, BA, CA, DA, EA, FA, GA, HA, Z0, Z1, Z2, Z3, Z4, Z5, Z6, Z7
    // insert IA, JA, KA, LA, MA, NA, OA
    // storage: ... | AA, BA, CA, DA, EA, FA, GA, HA, IA, JA, KA, LA, MA, NA, OA | ... (center border-node has 15 keys)

    // test is same

    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_OK(enter(s1));
    ASSERT_OK(enter(s2));

    // setup
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    for (char c = '0'; c <= '7'; c++) {
        char buf[2] = { '0', c };
        ASSERT_OK(insert(s1, st, std::string_view(buf, 2U), "0")) << c;
    }
    for (char c = 'A'; c <= 'H'; c++) {
        char buf[2] = { c, 'A' };
        ASSERT_OK(insert(s1, st, std::string_view(buf, 2U), "0")) << c;
    }
    // inserting "HA" causes split: 00, ..., 07 | AA, ..., HA
    for (char c = '0'; c <= '7'; c++) {
        char buf[2] = { 'Z', c };
        ASSERT_OK(insert(s1, st, std::string_view(buf, 2U), "0")) << c;
    }
    // inserting "Z7" causes split: AA, ..., HA | Z0, ..., Z7
    for (char c = 'I'; c <= 'O'; c++) {
        char buf[2] = { c, 'A' };
        ASSERT_OK(insert(s1, st, std::string_view(buf, 2U), "0")) << c;
    }
    // ... | AA, ..., OA | ...
    ASSERT_OK(commit(s1));

    // test
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(tx_begin({s2, transaction_options::transaction_type::SHORT}));

    wait_epoch_update();

    full_scan(s1, st);
    ASSERT_OK(insert(s1, st, "AB", "1")); // split
    ASSERT_OK(insert(s2, st, "NB", "2")); // into right
    ASSERT_OK(commit(s2));
    ASSERT_EQ(commit(s1), Status::ERR_CC);

    ASSERT_OK(leave(s1));
    ASSERT_OK(leave(s2));
}

// issue 1453
TEST_F(short_insert_scan_split_test, phantom_multitx_l1root_right) {
    // phantom if another tx inserts into right split node
    // split position: Layer1+ btree root border

    Storage st{};
    ASSERT_OK(create_storage("", st));
    Token s1{};
    Token s2{};
    ASSERT_OK(enter(s1));
    ASSERT_OK(enter(s2));

    // setup
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    for (char c = 'A'; c <= 'O'; c++) {
        char buf[10] = { '0', '1', '2', '3', '4', '5', '6', '7', c, 'A' };
        ASSERT_OK(upsert(s1, st, std::string_view(buf, 10U), "0")) << c;
    }
    ASSERT_OK(commit(s1));

    // test
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(tx_begin({s2, transaction_options::transaction_type::SHORT}));

    wait_epoch_update();

    full_scan(s1, st);
    ASSERT_OK(insert(s1, st, "01234567AB", "1")); // split
    ASSERT_OK(insert(s2, st, "01234567NB", "2")); // into right
    ASSERT_OK(commit(s2));
    ASSERT_EQ(commit(s1), Status::ERR_CC);

    ASSERT_OK(leave(s1));
    ASSERT_OK(leave(s2));
}

// issue 1453
TEST_F(short_insert_scan_split_test, phantom_multitx_nonserial_mtroot_right) {
    // like phantom_multitx_mtroot_right
    // abort either tx in non-serial two txs

    // setup
    // st1: AA, BA, CA, DA, EA, FA, GA, HA, IA, JA, KA, LA, MA, NA, OA (15 keys)
    // st2: AA, BA, CA, DA, EA, FA, GA, HA, IA, JA, KA, LA, MA, NA, OA (15 keys)

    // expect
    // s1: begin                       -> OK
    // s2: begin                       -> OK
    // s1: full scan st1               -> OK
    // s2: full scan st2               -> OK
    // s1: insert st1 AB               -> OK
    // s2: insert st1 NB               -> OK
    // s2: insert st2 AB               -> OK
    // s1: insert st2 NB               -> OK
    // s2: commit                      -> OK (or ERR_CC)
    // s1: commit                      -> ERR_CC (or OK)
    // either s1 or s2 is aborted by ERR_CC (maybe both by false-positive)

    Storage st1{};
    Storage st2{};
    ASSERT_OK(create_storage("1", st1));
    ASSERT_OK(create_storage("2", st2));
    Token s1{};
    Token s2{};
    ASSERT_OK(enter(s1));
    ASSERT_OK(enter(s2));

    // setup
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    for (char c = 'A'; c <= 'O'; c++) {
        char buf[2] = { c, 'A' };
        ASSERT_OK(upsert(s1, st1, std::string_view(buf, 2U), "0")) << c;
        ASSERT_OK(upsert(s1, st2, std::string_view(buf, 2U), "0")) << c;
    }
    ASSERT_OK(commit(s1));

    // test
    ASSERT_OK(tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(tx_begin({s2, transaction_options::transaction_type::SHORT}));

    wait_epoch_update();

    full_scan(s1, st1);
    full_scan(s2, st2);
    ASSERT_OK(insert(s1, st1, "AB", "1")); // split in st1
    ASSERT_OK(insert(s2, st1, "NB", "2"));
    ASSERT_OK(insert(s2, st2, "AB", "2")); // split in st2
    ASSERT_OK(insert(s1, st2, "NB", "1"));
    auto rc2 = commit(s2);
    auto rc1 = commit(s1);
    ASSERT_TRUE(rc1 == Status::ERR_CC || rc2 == Status::ERR_CC) << "rc1:" << rc1 << ", rc2:" << rc2;

    ASSERT_OK(leave(s1));
    ASSERT_OK(leave(s2));
}

} // namespace shirakami::testing
