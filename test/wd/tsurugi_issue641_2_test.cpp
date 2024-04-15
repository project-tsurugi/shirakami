
#include <atomic>
#include <functional>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/garbage.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tid.h"

#include "shirakami/interface.h"

#include "index/yakushima/include/interface.h"

#include "test_tool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

namespace shirakami::testing {

class tsurugi_issue641_2_test : public ::testing::TestWithParam<bool> {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue641_2_test");
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

TEST_F(tsurugi_issue641_2_test, // NOLINT
       version_order_test) {
    // epoch lock
    {
        std::unique_lock<std::mutex> lk{epoch::get_ep_mtx()};
        // set max epoch in tid_word
        epoch::set_global_epoch(std::pow(2, tid_word::bit_size_epoch) - 1);
        LOG(INFO) << epoch::get_global_epoch();

        // write
        Storage st{};
        ASSERT_OK(create_storage("", st));
        Token t{};
        ASSERT_OK(enter(t));
        ASSERT_OK(tx_begin({t, transaction_type::SHORT}));
        ASSERT_OK(upsert(t, st, "", "1"));
        ASSERT_OK(commit(t));

        // write new version
        // inc epoch
        epoch::set_global_epoch(epoch::get_global_epoch() + 1);
        LOG(INFO) << epoch::get_global_epoch();
        ASSERT_OK(tx_begin({t, transaction_type::SHORT}));
        ASSERT_OK(upsert(t, st, "", "2"));
        ASSERT_OK(commit(t));
        // check tid
        Record* rec_ptr{};
        std::pair<yakushima::node_version64_body, yakushima::node_version64*>
                checked_version{};
        ASSERT_OK(get<Record>(st, "", rec_ptr, &checked_version));
        tid_word tid = rec_ptr->get_tidw_ref();
        LOG(INFO) << tid;

        // check latest version
        ASSERT_OK(tx_begin({t, transaction_type::SHORT}));
        std::string buf{};
        ASSERT_OK(search_key(t, st, "", buf));
        ASSERT_EQ(buf, "2");
        ASSERT_OK(commit(t));
        // check tid
        tid = rec_ptr->get_tidw_ref();
        LOG(INFO) << tid;
        /**
         * version order is valid at occ. epoch bit keeps full and not change 
         * because epoch bit is highest bit and occ(silo) select max value in
         * 1: epoch, 2: max of rw set + 1, 3: most recently choosen +1.
         * 
         * LTX is not the mechanism
        */
        ASSERT_OK(tx_begin({t, transaction_type::LONG, {st}}));
        // inc epoch for start
        epoch::set_global_epoch(epoch::get_global_epoch() + 1);
        LOG(INFO) << epoch::get_global_epoch();
        ASSERT_OK(upsert(t, st, "", "3"));
        ASSERT_OK(commit(t));
        // check 3
        ASSERT_OK(tx_begin({t, transaction_type::SHORT}));
        ASSERT_OK(search_key(t, st, "", buf));
        ASSERT_EQ(buf, "3");
        ASSERT_OK(commit(t));
        tid = rec_ptr->get_tidw_ref();
        LOG(INFO) << tid;

        // cleanup
        ASSERT_OK(leave(t));
    }
}

} // namespace shirakami::testing