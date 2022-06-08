
#include <xmmintrin.h>

#include <bitset>
#include <future>
#include <mutex>

#include "concurrency_control/wp/include/epoch.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class long_update_after_short_delete : public ::testing::Test { // NOLINT
public:
    static void call_once_f() { FLAGS_stderrthreshold = 0; }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

inline void wait_epoch_update() {
    auto ce{epoch::get_global_epoch()};
    for (;;) {
        if (ce != epoch::get_global_epoch()) { break; }
        _mm_pause();
    }
}

TEST_F(long_update_after_short_delete, independent_tx) { // NOLINT
    Storage st{};
    register_storage(st);
    std::string k("k"); // NOLINT
    std::string v("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, st, k));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, tx_begin(s, TX_TYPE::LONG, {st}));
    wait_epoch_update();
    ASSERT_EQ(Status::WARN_NOT_FOUND, update(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
