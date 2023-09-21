
#include <atomic>
#include <functional>

#include "shirakami/interface.h"
#include "test_tool.h"

#include "glog/logging.h"
#include "gtest/gtest.h"


using namespace shirakami;
using transaction_type = shirakami::transaction_options::transaction_type;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class tsurugi_issue325 : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue325");
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

TEST_F(tsurugi_issue325, check_durability_callback) { // NOLINT
    std::size_t c_counter{0};
    register_durability_callback([c_counter](durability_marker_type dm) {
        LOG(INFO) << c_counter << ", " << dm;
    });
    ++c_counter;
    register_durability_callback([c_counter](durability_marker_type dm) {
        LOG(INFO) << c_counter << ", " << dm;
    });
    sleep(1);
}

TEST_F(tsurugi_issue325, check_commit_callback) { // NOLINT
    Storage st{};
    ASSERT_OK(create_storage("test", st));
    Token s{};
    Token s2{};
    ASSERT_OK(enter(s));
    ASSERT_OK(enter(s2));

    std::atomic<std::size_t> wait_callback{0};
    // commit callback
    auto cb = [&wait_callback](Status rs, reason_code rc,
                               durability_marker_type dm) {
        LOG(INFO) << rs << ", " << rc << ", " << dm;
        --wait_callback;
    };

    // occ
    LOG(INFO) << "about occ";
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st, "", ""));
    ++wait_callback;
    ASSERT_TRUE(commit(s, cb));
    while (wait_callback != 0) { _mm_pause(); }

    // ltx
    LOG(INFO) << "about ltx";
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    ASSERT_OK(
            tx_begin({s2, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    std::string buf{};
    ASSERT_OK(search_key(s, st, "", buf));
    ASSERT_OK(upsert(s, st, "", ""));
    ASSERT_OK(search_key(s2, st, "", buf));
    ASSERT_OK(upsert(s2, st, "", ""));
    ++wait_callback;
    ++wait_callback;
    ASSERT_FALSE(commit(s2, cb));
    ASSERT_TRUE(commit(s, cb));
    while (wait_callback != 0) { _mm_pause(); }

    // read only
    LOG(INFO) << "about rtx";
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    wait_epoch_update();
    ASSERT_OK(search_key(s, st, "", buf));
    ++wait_callback;
    ASSERT_TRUE(commit(s, cb));
    while (wait_callback != 0) { _mm_pause(); }

    // cleanup
    ASSERT_OK(leave(s));
    ASSERT_OK(leave(s2));
}

} // namespace shirakami::testing
