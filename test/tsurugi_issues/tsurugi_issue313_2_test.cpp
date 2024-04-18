
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>
#include <sstream>
#include <thread>
#include <iomanip>
#include <mutex>
#include <string>

#include "test_tool.h"
#include "concurrency_control/include/epoch.h"
#include "shirakami/interface.h"
#include "gtest/gtest.h"
#include "shirakami/api_sequence.h"
#include "shirakami/api_storage.h"
#include "shirakami/binary_printer.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"

using namespace shirakami;



namespace shirakami::testing {

class tsurugi_issue313_2 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue313_2");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

static std::string mk_key(int i) {
    std::ostringstream ss;
    ss << std::setw(7) << std::setfill('0') << i; // NOLINT
    return ss.str();
}

// for issue#313, case2 (strange behaviour around sequence)
// * create_sequence() -> seqid0
// * OCC1 start tx, update seqid0 to {ver=1, val=1}
// * OCC2 start tx
// * OCC2 grow read_set_for_stx huge (for commit() takes long time)
// * OCC2 update seqid0 to {2, 2}
// * epoch = e0
// * OCC2 start commit(), take long time... (not finished)
// * epoch++ (e0 -> e1)
// * OCC1 commit
//     * sequence::sequence_map = {seqid0 -> { ..., e1 -> {1, 1} }}
// * OCC2 ...commit() finished
//     * sequence::sequence_map = {seqid0 -> { ..., e0 -> {2, 2}, e1 -> {1, 1} }}  <- XXX: order reversal

TEST_F(tsurugi_issue313_2, sequence_epoch_ver_order) { // NOLINT

    int n = 655360; // set this for enough size // NOLINT
    SequenceId seqid{};
    ASSERT_OK(create_sequence(&seqid));

    auto set_seq = [seqid](Token s, int v) {
        SequenceVersion seqver{};
        SequenceValue seqval{};
        read_sequence(seqid, &seqver, &seqval);
        ASSERT_OK(update_sequence(s, seqid, v, v));
    };
    auto commit_leave = [](Token s, bool wait = false) { // NOLINT
        if (wait) { wait_epoch_update(); }
        LOG(INFO) << wait;
        LOG(INFO) << "commit begin " << s;
        ASSERT_OK(commit(s));
        LOG(INFO) << "commit end " << s;
        ASSERT_OK(leave(s));
    };

    Storage st{};
    ASSERT_OK(create_storage("", st));

    // setup many records for search_key
    Token s{};
    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin(s));
    auto epoch_insert_start = epoch::get_global_epoch();
    for (int i = 0; i < n; i++) {
        ASSERT_OK(insert(s, st, mk_key(i), "val"));
        if (i % 1000 == 0) {                                           // NOLINT
            if (epoch::get_global_epoch() - epoch_insert_start > 20) { // NOLINT
                LOG(INFO) << "takes too long time, shrink n " << n << " to "
                          << i;
                n = i;
                break;
            }
        }
    }
    ASSERT_OK(commit(s));
    ASSERT_OK(leave(s));
    LOG(INFO) << "setup done";

    Token s1{};
    ASSERT_OK(enter(s1));
    ASSERT_OK(tx_begin(s1));
    set_seq(s1, 1);
    Token s2{};
    ASSERT_OK(enter(s2));
    ASSERT_OK(tx_begin(s2));
    // grow read_set_for_stx
    for (int i = 0; i < n; i++) {
        std::string val{};
        ASSERT_OK(search_key(s2, st, mk_key(i), val));
    }
    set_seq(s2, 2);

    std::thread th1(commit_leave, s1, true);
    std::thread th2(commit_leave, s2);
    th1.join();
    th2.join();

    sleep(1);
    SequenceId sid{seqid};
    SequenceVersion sver{};
    SequenceValue sval{};
    LOG(INFO);
    ASSERT_OK(read_sequence(sid, &sver, &sval));
    ASSERT_EQ(sver, 2);
    ASSERT_EQ(sval, 2);
}

} // namespace shirakami::testing
