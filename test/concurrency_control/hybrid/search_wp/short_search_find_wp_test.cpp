
#include <emmintrin.h>
#include <mutex>
#include <string>

#include "gtest/gtest.h"
#include "concurrency_control/include/epoch.h"
#include "shirakami/interface.h"
#include "glog/logging.h"
#include "shirakami/api_storage.h"
#include "shirakami/binary_printer.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"
#include "shirakami/transaction_options.h"

namespace shirakami::testing {

using namespace shirakami;

class search_wp : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-hybrid-search_wp-"
                "search_wp_test");
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

void wait_change_epoch() {
    auto ce{epoch::get_global_epoch()};
    for (;;) {
        if (ce != epoch::get_global_epoch()) { break; }
        _mm_pause();
    }
}

TEST_F(search_wp, short_search_find_wp) { // NOLINT
    Token short_s{};
    Token long_s{};
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    ASSERT_EQ(Status::OK, enter(short_s));
    ASSERT_EQ(Status::OK, enter(long_s));
    ASSERT_EQ(Status::OK, tx_begin({long_s,
                                    transaction_options::transaction_type::LONG,
                                    {st}}));
    wait_change_epoch(); // valid ^^ wp.
    std::string vb{};
    ASSERT_EQ(
            Status::OK,
            tx_begin({short_s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::ERR_CC, search_key(short_s, st, "", vb));
    ASSERT_EQ(Status::OK, leave(short_s));
    ASSERT_EQ(Status::OK, leave(long_s));
}

} // namespace shirakami::testing
