
#include <mutex>
#include <vector>

#include "clock.h"

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/read_plan.h"
#include "concurrency_control/include/session.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class tsurugi_issue344 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue344");
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

TEST_F(tsurugi_issue344, from_jogasaki_sql_test) { // NOLINT
    { GTEST_SKIP() << "LONG is not supported"; }
    Storage st_system{};
    ASSERT_OK(create_storage("__system_sequence", st_system));

    Storage st_system_out{};
    ASSERT_OK(get_storage("__system_sequence", st_system_out));

    storage_option out_stop;
    ASSERT_OK(storage_get_options(st_system, out_stop));

    Storage st_char_tab{};
    ASSERT_OK(create_storage("CHAR_TAB", st_char_tab));
    Storage st_int4_tab{};
    ASSERT_OK(create_storage("INT4_TAB", st_int4_tab));
    Storage st_non_nullables{};
    ASSERT_OK(create_storage("NON_NULLABLES", st_non_nullables));
    Storage st_t0{};
    ASSERT_OK(create_storage("T0", st_t0));
    Storage st_t1{};
    ASSERT_OK(create_storage("T1", st_t1));
    Storage st_t10{};
    ASSERT_OK(create_storage("T10", st_t10));
    Storage st_t2{};
    ASSERT_OK(create_storage("T2", st_t2));
    Storage st_t20{};
    ASSERT_OK(create_storage("T20", st_t20));
    Storage st_tdecimals{};
    ASSERT_OK(create_storage("TDECIMALS", st_tdecimals));
    Storage st_tsecondary{};
    ASSERT_OK(create_storage("TSECONDARY", st_tsecondary));
    Storage st_tsecondary_i1{};
    ASSERT_OK(create_storage("TSECONDARY_I1", st_tsecondary_i1));
    Storage st_tseq0{};
    ASSERT_OK(create_storage("TSEQ0", st_tseq0));
    Storage st_tseq1{};
    ASSERT_OK(create_storage("TSEQ1", st_tseq1));
    Storage st_ttemporals{};
    ASSERT_OK(create_storage("TTEMPORALS", st_ttemporals));

    Token t1{};
    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin({t1, transaction_options::transaction_type::SHORT}));
    ScanHandle shd{};
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              open_scan(t1, st_system, "", scan_endpoint::INF, "",
                        scan_endpoint::INF, shd));
    SequenceId sid;
    ASSERT_OK(create_sequence(&sid));
    SequenceVersion sver{};
    SequenceValue sval{};
    ASSERT_OK(read_sequence(sid, &sver, &sval));
    ASSERT_OK(create_sequence(&sid));
    ASSERT_OK(read_sequence(sid, &sver, &sval));
    ASSERT_OK(upsert(t1, st_system, "2", "~\x7f\xff\xff\xff\xff\xff\xff\xfd"));
    ASSERT_OK(upsert(t1, st_system, "1", "~\x7f\xff\xff\xff\xff\xff\xff\xfe"));
    ASSERT_OK(commit(t1));
    ASSERT_OK(leave(t1));

    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin(
            {t1,
             transaction_options::transaction_type::LONG,
             {st_char_tab, st_int4_tab, st_non_nullables, st_t0, st_t1, st_t10,
              st_t2, st_t20, st_tdecimals, st_tsecondary, st_tsecondary_i1,
              st_tseq0, st_tseq1, st_ttemporals, st_system}}));
    wait_epoch_update();
    ASSERT_OK(insert(t1, st_non_nullables, "1",
                     "~?\xdb\xff\xff\xff\xff\xff\xff"));
    ASSERT_OK(commit(t1));
    ASSERT_OK(leave(t1));

    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin(
            {t1,
             transaction_options::transaction_type::LONG,
             {st_char_tab, st_int4_tab, st_non_nullables, st_t0, st_t1, st_t10,
              st_t2, st_t20, st_tdecimals, st_tsecondary, st_tsecondary_i1,
              st_tseq0, st_tseq1, st_ttemporals, st_system}}));
    wait_epoch_update();
    ASSERT_OK(insert(t1, st_non_nullables, "2",
                     "~?\xcb\xff\xff\xff\xff\xff\xff"));
    ASSERT_OK(commit(t1));
    ASSERT_OK(leave(t1));

    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin(
            {t1,
             transaction_options::transaction_type::LONG,
             {st_char_tab, st_int4_tab, st_non_nullables, st_t0, st_t1, st_t10,
              st_t2, st_t20, st_tdecimals, st_tsecondary, st_tsecondary_i1,
              st_tseq0, st_tseq1, st_ttemporals, st_system}}));
    wait_epoch_update();
    ASSERT_OK(insert(t1, st_t10, "3", "~?\xc1\xff\xff\xff\xff\xff\xff"));
    ASSERT_OK(commit(t1));
    ASSERT_OK(leave(t1));

    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin(
            {t1,
             transaction_options::transaction_type::LONG,
             {st_char_tab, st_int4_tab, st_non_nullables, st_t0, st_t1, st_t10,
              st_t2, st_t20, st_tdecimals, st_tsecondary, st_tsecondary_i1,
              st_tseq0, st_tseq1, st_ttemporals, st_system}}));
    wait_epoch_update();
    ASSERT_OK(insert(t1, st_t10, "4", "~?\xbb\xff\xff\xff\xff\xff\xff"));
    ASSERT_OK(commit(t1));
    ASSERT_OK(leave(t1));

    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin(
            {t1,
             transaction_options::transaction_type::LONG,
             {st_char_tab, st_int4_tab, st_non_nullables, st_t0, st_t1, st_t10,
              st_t2, st_t20, st_tdecimals, st_tsecondary, st_tsecondary_i1,
              st_tseq0, st_tseq1, st_ttemporals, st_system}}));
    wait_epoch_update();
    ASSERT_OK(insert(t1, st_t10, "5", "~?\xb6\xff\xff\xff\xff\xff\xff"));
    ASSERT_OK(commit(t1));
    ASSERT_OK(leave(t1));

    ASSERT_OK(enter(t1));
    ASSERT_OK(tx_begin(
            {t1,
             transaction_options::transaction_type::LONG,
             {st_char_tab, st_int4_tab, st_non_nullables, st_t0, st_t1, st_t10,
              st_t2, st_t20, st_tdecimals, st_tsecondary, st_tsecondary_i1,
              st_tseq0, st_tseq1, st_ttemporals, st_system}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, open_scan(t1, st_system, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, shd));
    std::string buf{};
    ASSERT_OK(read_key_from_scan(t1, shd, buf));
    ASSERT_OK(read_value_from_scan(t1, shd, buf));
    ASSERT_OK(next(t1, shd));
    ASSERT_OK(read_key_from_scan(t1, shd, buf));
    ASSERT_OK(read_value_from_scan(t1, shd, buf));
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(t1, shd));
    ASSERT_OK(close_scan(t1, shd));
    ASSERT_EQ(Status::OK, open_scan(t1, st_t10, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, shd));
    ASSERT_OK(read_key_from_scan(t1, shd, buf));
    ASSERT_OK(read_value_from_scan(t1, shd, buf));
    ASSERT_OK(next(t1, shd));
    ASSERT_OK(read_key_from_scan(t1, shd, buf));
    ASSERT_OK(read_value_from_scan(t1, shd, buf));
    ASSERT_OK(next(t1, shd));
    ASSERT_OK(read_key_from_scan(t1, shd, buf));
    ASSERT_OK(read_value_from_scan(t1, shd, buf));
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(t1, shd));
    ASSERT_OK(commit(t1));
    ASSERT_OK(leave(t1));
}

} // namespace shirakami::testing
