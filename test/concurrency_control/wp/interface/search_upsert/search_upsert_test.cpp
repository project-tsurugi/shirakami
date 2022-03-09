
#include <xmmintrin.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <climits>
#include <mutex>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"

#include "concurrency_control/wp/include/epoch.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class search_upsert : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "search_upsert-search_upsert_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/search_upsert_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

inline void wait_epoch_update() {
    epoch::epoch_t ce{epoch::get_global_epoch()};
    for (;;) {
        if (ce == epoch::get_global_epoch()) {
            _mm_pause();
        } else {
            break;
        }
    }
}

TEST_F(search_upsert, short_search_find_valid_wp) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token ss{}; // short
    Token sb{}; // long
    ASSERT_EQ(enter(ss), Status::OK);
    ASSERT_EQ(enter(sb), Status::OK);
    // prepare data
    ASSERT_EQ(Status::OK, upsert(ss, st, "", ""));
    ASSERT_EQ(Status::OK, commit(ss)); // NOLINT
    ASSERT_EQ(tx_begin(ss), Status::OK);
    ASSERT_EQ(tx_begin(sb, false, true, {st}), Status::OK);
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(Status::WARN_CONFLICT_ON_WRITE_PRESERVE,
              search_key(ss, st, "", vb));
    ASSERT_EQ(Status::OK, commit(ss)); // NOLINT
    ASSERT_EQ(Status::OK, upsert(sb, st, "", ""));
    ASSERT_EQ(Status::OK, commit(sb)); // NOLINT
    ASSERT_EQ(leave(ss), Status::OK);
    ASSERT_EQ(leave(sb), Status::OK);
}

TEST_F(search_upsert, short_search_finish_before_valid_wp) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token ss{}; // short
    Token sb{}; // long
    ASSERT_EQ(enter(ss), Status::OK);
    ASSERT_EQ(enter(sb), Status::OK);
    // prepare data
    ASSERT_EQ(Status::OK, upsert(ss, st, "", ""));
    ASSERT_EQ(Status::OK, commit(ss)); // NOLINT
    {
        std::unique_lock<std::mutex> lk{epoch::get_ep_mtx()};
        ASSERT_EQ(tx_begin(ss), Status::OK);
        ASSERT_EQ(tx_begin(sb, false, true, {st}), Status::OK);
        std::string vb{};
        ASSERT_EQ(Status::OK, search_key(ss, st, "", vb));
        ASSERT_EQ(Status::OK, commit(ss)); // NOLINT
    }
    wait_epoch_update();
    ASSERT_EQ(Status::OK, upsert(sb, st, "", ""));
    ASSERT_EQ(Status::OK, commit(sb)); // NOLINT
    ASSERT_EQ(leave(ss), Status::OK);
    ASSERT_EQ(leave(sb), Status::OK);
}

TEST_F(search_upsert, short_search_finish_after_valid_wp) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token ss{}; // short
    Token sb{}; // long
    ASSERT_EQ(enter(ss), Status::OK);
    ASSERT_EQ(enter(sb), Status::OK);
    // prepare data
    ASSERT_EQ(Status::OK, upsert(ss, st, "", ""));
    ASSERT_EQ(Status::OK, commit(ss)); // NOLINT
    {
        std::unique_lock<std::mutex> lk{epoch::get_ep_mtx()};
        ASSERT_EQ(tx_begin(ss), Status::OK);
        ASSERT_EQ(tx_begin(sb, false, true, {st}), Status::OK);
        std::string vb{};
        ASSERT_EQ(Status::OK, search_key(ss, st, "", vb));
    }
    wait_epoch_update();
    ASSERT_EQ(Status::ERR_VALIDATION, commit(ss)); // NOLINT
    // due to wp
    ASSERT_EQ(Status::OK, upsert(sb, st, "", ""));
    ASSERT_EQ(Status::OK, commit(sb)); // NOLINT
    ASSERT_EQ(leave(ss), Status::OK);
    ASSERT_EQ(leave(sb), Status::OK);
}

TEST_F(search_upsert, short_long_conflict) { // NOLINT
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    Token s{};  // short
    Token s2{}; // long
    ASSERT_EQ(enter(s), Status::OK);
    ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(enter(s2), Status::OK);
    ASSERT_EQ(tx_begin(s2, false, true, {st}), Status::OK);
    std::string vb{};
    while (search_key(s2, st, "", vb) == Status::WARN_PREMATURE) {
        _mm_pause();
    }
    ASSERT_EQ(leave(s), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

} // namespace shirakami::testing
