
#include <xmmintrin.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <climits>
#include <mutex>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "clock.h"

#include "concurrency_control/wp/include/epoch.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class write_skew : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "search_upsert-write_skew_test");
        FLAGS_stderrthreshold = 0;
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/tmp/write_skew_test_log");
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(false, log_dir_); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
    static inline std::string log_dir_;        // NOLINT
};

TEST_F(write_skew, simple) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token s1{};
    Token s2{};
    std::string x{"x"};
    std::string y{"y"};
    ASSERT_EQ(enter(s1), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    // prepare data
    std::size_t v{0};
    std::string_view v_view{reinterpret_cast<char*>(&v), sizeof(v)}; // NOLINT
    ASSERT_EQ(upsert(s1, st, x, v_view), Status::OK);
    ASSERT_EQ(upsert(s1, st, y, v_view), Status::OK);
    ASSERT_EQ(Status::OK, commit(s1));

    // stop epoch
    // epoch align 2 tx.
    epoch::get_ep_mtx().lock();
    ASSERT_EQ(tx_begin(s1, false, true, {st}), Status::OK);
    ASSERT_EQ(tx_begin(s2, false, true, {st}), Status::OK);
    epoch::get_ep_mtx().unlock();

    // wait change epoch
    sleepMs(PARAM_EPOCH_TIME * 2);

    // read phase
    std::string vb1{};
    std::string vb2{};
    ASSERT_EQ(search_key(s1, st, x, vb1), Status::OK);
    ASSERT_EQ(search_key(s2, st, y, vb2), Status::ERR_FAIL_WP);
    std::size_t v1{};
    //std::size_t v2{};
    std::string vb{};
    vb = vb1;
    memcpy(&v1, vb.data(), sizeof(v1));
    ++v1;
    //1memcpy(&v2, tuple2->get_value().data(), sizeof(v2));
    //1++v2;
    std::string v1_view{reinterpret_cast<char*>(&v1), sizeof(v1)}; // NOLINT
    //std::string v2_view{reinterpret_cast<char*>(&v2), sizeof(v2)};
    ASSERT_EQ(upsert(s1, st, y, v1_view), Status::OK);
    //ASSERT_EQ(upsert(s2, st, x, v2_view), Status::OK);

    // commit phase
    ASSERT_EQ(commit(s1), Status::OK);
    //ASSERT_NE(commit(s2), Status::OK);


    ASSERT_EQ(leave(s1), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

} // namespace shirakami::testing
