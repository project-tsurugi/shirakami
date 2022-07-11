
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

class search : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "search-search_test");
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

// todo after RO
TEST_F(search, DISABLED_read_only_mode_single_long_search_success) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(create_storage(st), Status::OK);
    Token s{};
    ASSERT_EQ(enter(s), Status::OK);

    // prepare data
    ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s));
    wait_epoch_update();

    // test
    // read only mode and long tx mode, single search
    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::READ_ONLY}), Status::OK);
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(search_key(s, st, "", vb), Status::OK);
    ASSERT_EQ(Status::OK, commit(s));

    // clean up test
    ASSERT_EQ(leave(s), Status::OK);
}

TEST_F(search, read_write_mode_single_long_search_success) { // NOLINT
    // prepare test
    Storage st{};
    ASSERT_EQ(create_storage(st), Status::OK);
    Token s{};
    ASSERT_EQ(enter(s), Status::OK);

    // prepare data
    ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s));
    wait_epoch_update();

    // test
    // read only mode and long tx mode, single search
    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::LONG}), Status::OK);
    wait_epoch_update();
    std::string vb{};
    ASSERT_EQ(search_key(s, st, "", vb), Status::OK);
    ASSERT_EQ(Status::OK, commit(s));

    // clean up test
    ASSERT_EQ(leave(s), Status::OK);
}

} // namespace shirakami::testing
