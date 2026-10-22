
#include <mutex>
#include <string_view>
#include <vector>

#include "shirakami/interface.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"
#include "shirakami/transaction_options.h"

#include "test_tool.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"

namespace shirakami::testing {

using namespace shirakami;
using namespace std::literals;

class long_update_local_read_range_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-helper-"
                                  "long_update_local_read_range_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

#define EXPECT_RANGE_EQ(ti, wp_meta_ptr, l_key, l_end, r_key, r_end) do { \
        auto range_read = std::get<1>(ti->get_overtaken_ltx_set()[wp_meta_ptr]); \
        if (l_key != "***"sv) { EXPECT_EQ(std::get<0>(range_read), l_key); } \
        EXPECT_EQ(std::get<1>(range_read), l_end); \
        if (r_key != "***"sv) { EXPECT_EQ(std::get<2>(range_read), r_key); } \
        EXPECT_EQ(std::get<3>(range_read), r_end); \
    } while (false)

TEST_F(long_update_local_read_range_test, simple1) {
    // XXX: replace by "using enum scan_endpoint" in C++20
    constexpr auto INCLUSIVE = scan_endpoint::INCLUSIVE;
    constexpr auto EXCLUSIVE = scan_endpoint::EXCLUSIVE;
    constexpr auto INF = scan_endpoint::INF;
    Token s{};
    ASSERT_OK(enter(s));
    Storage st{};
    ASSERT_OK(create_storage("1", st));
    wp::wp_meta* wp_meta_ptr{};
    ASSERT_OK(wp::find_wp_meta(st, wp_meta_ptr));

    std::vector<Storage> wp{st};
    // wp for existing storage

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::LONG, wp}));
    {
        session* ti = static_cast<session*>(s);

        long_tx::update_local_read_range(ti, wp_meta_ptr, "b", INCLUSIVE, "c", INCLUSIVE);
        EXPECT_RANGE_EQ(ti, wp_meta_ptr, "b", INCLUSIVE, "c", INCLUSIVE);
        // append to right
        long_tx::update_local_read_range(ti, wp_meta_ptr, "c", INCLUSIVE, "d", INCLUSIVE);
        EXPECT_RANGE_EQ(ti, wp_meta_ptr, "b", INCLUSIVE, "d", INCLUSIVE);
        // append to left
        long_tx::update_local_read_range(ti, wp_meta_ptr, "a", INCLUSIVE, "b", INCLUSIVE);
        EXPECT_RANGE_EQ(ti, wp_meta_ptr, "a", INCLUSIVE, "d", INCLUSIVE);
    }
    ASSERT_OK(abort(s));

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::LONG, wp}));
    {
        session* ti = static_cast<session*>(s);
        long_tx::update_local_read_range(ti, wp_meta_ptr, "b", INCLUSIVE, "c", INCLUSIVE);
        EXPECT_RANGE_EQ(ti, wp_meta_ptr, "b", INCLUSIVE, "c", INCLUSIVE);
        // right INF
        long_tx::update_local_read_range(ti, wp_meta_ptr, "c", INCLUSIVE, "", INF);
        EXPECT_RANGE_EQ(ti, wp_meta_ptr, "b", INCLUSIVE, "***", INF);
        // left INF
        long_tx::update_local_read_range(ti, wp_meta_ptr, "", INF, "b", INCLUSIVE);
        EXPECT_RANGE_EQ(ti, wp_meta_ptr, "***", INF, "***", INF);
    }
    ASSERT_OK(abort(s));

    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::LONG, wp}));
    {
        session* ti = static_cast<session*>(s);
        long_tx::update_local_read_range(ti, wp_meta_ptr, "b", EXCLUSIVE, "c", EXCLUSIVE);
        EXPECT_RANGE_EQ(ti, wp_meta_ptr, "b", EXCLUSIVE, "c", EXCLUSIVE);

        // same key, INC : EXC -> INC
        long_tx::update_local_read_range(ti, wp_meta_ptr, "b", EXCLUSIVE, "c", INCLUSIVE);
        EXPECT_RANGE_EQ(ti, wp_meta_ptr, "b", EXCLUSIVE, "c", INCLUSIVE);
        // same key, EXC : keep INC
        long_tx::update_local_read_range(ti, wp_meta_ptr, "b", EXCLUSIVE, "c", EXCLUSIVE);
        EXPECT_RANGE_EQ(ti, wp_meta_ptr, "b", EXCLUSIVE, "c", INCLUSIVE);
        // different key, EXC
        long_tx::update_local_read_range(ti, wp_meta_ptr, "b", EXCLUSIVE, "d", EXCLUSIVE);
        EXPECT_RANGE_EQ(ti, wp_meta_ptr, "b", EXCLUSIVE, "d", EXCLUSIVE);

        // same key, INC : EXC -> INC
        long_tx::update_local_read_range(ti, wp_meta_ptr, "b", INCLUSIVE, "c", EXCLUSIVE);
        EXPECT_RANGE_EQ(ti, wp_meta_ptr, "b", INCLUSIVE, "d", EXCLUSIVE);
        // same key, EXC : keep INC
        long_tx::update_local_read_range(ti, wp_meta_ptr, "b", EXCLUSIVE, "c", EXCLUSIVE);
        EXPECT_RANGE_EQ(ti, wp_meta_ptr, "b", INCLUSIVE, "d", EXCLUSIVE);
        // different key, EXC
        long_tx::update_local_read_range(ti, wp_meta_ptr, "a", EXCLUSIVE, "c", EXCLUSIVE);
        EXPECT_RANGE_EQ(ti, wp_meta_ptr, "a", EXCLUSIVE, "d", EXCLUSIVE);
    }
    ASSERT_OK(abort(s));

    ASSERT_OK(leave(s));
}

} // namespace shirakami::testing
