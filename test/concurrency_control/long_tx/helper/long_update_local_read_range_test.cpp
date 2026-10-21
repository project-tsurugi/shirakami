
#include <mutex>
#include <vector>

#include "shirakami/interface.h"
#include "shirakami/api_storage.h"
#include "shirakami/binary_printer.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"
#include "shirakami/transaction_options.h"

#include "test_tool.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"

namespace shirakami::testing {

using namespace shirakami;

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
        EXPECT_EQ(std::get<0>(range_read), l_key); \
        EXPECT_EQ(std::get<1>(range_read), l_end); \
        EXPECT_EQ(std::get<2>(range_read), r_key); \
        EXPECT_EQ(std::get<3>(range_read), r_end); \
    } while (false)

TEST_F(long_update_local_read_range_test, simple1) {
    Token s{};
    ASSERT_OK(enter(s));
    Storage st{};
    ASSERT_OK(create_storage("1", st));
    std::vector<Storage> wp{st};
    // wp for existing storage
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::LONG, wp}));

    auto* ti = static_cast<session*>(s);
    wp::wp_meta* wp_meta_ptr{};
    ASSERT_OK(wp::find_wp_meta(st, wp_meta_ptr));

    long_tx::update_local_read_range(ti, wp_meta_ptr, "a", scan_endpoint::INCLUSIVE, "b", scan_endpoint::INCLUSIVE);
    EXPECT_RANGE_EQ(ti, wp_meta_ptr, "a", scan_endpoint::INCLUSIVE, "b", scan_endpoint::INCLUSIVE);
    long_tx::update_local_read_range(ti, wp_meta_ptr, "b", scan_endpoint::INCLUSIVE, "c", scan_endpoint::INCLUSIVE);
    EXPECT_RANGE_EQ(ti, wp_meta_ptr, "a", scan_endpoint::INCLUSIVE, "c", scan_endpoint::INCLUSIVE);

    ASSERT_OK(abort(s));
    ASSERT_OK(leave(s));
}

} // namespace shirakami::testing
