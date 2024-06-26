
#include <mutex>
#include <cstring>
#include <string>
#include <string_view>

#include "atomic_wrapper.h"
#include "test_tool.h"
#include "shirakami/interface.h"
#include "gtest/gtest.h"
#include "glog/logging.h"
#include "shirakami/api_storage.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"
#include "shirakami/transaction_options.h"

namespace shirakami::testing {

using namespace shirakami;

class write_skew : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "search_upsert-write_skew_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(write_skew, simple) { // NOLINT
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s1{};
    Token s2{};
    std::string x{"x"};
    std::string y{"y"};
    ASSERT_EQ(enter(s1), Status::OK);
    ASSERT_EQ(enter(s2), Status::OK);
    // prepare data
    std::size_t v{0};
    std::string_view v_view{reinterpret_cast<char*>(&v), sizeof(v)}; // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(upsert(s1, st, x, v_view), Status::OK);
    ASSERT_EQ(upsert(s1, st, y, v_view), Status::OK);
    ASSERT_EQ(Status::OK, commit(s1));

    // stop epoch
    // epoch align 2 tx.
    stop_epoch();
    ASSERT_EQ(tx_begin({s1, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    ASSERT_EQ(tx_begin({s2, transaction_options::transaction_type::LONG, {st}}),
              Status::OK);
    resume_epoch();

    // wait change epoch
    wait_epoch_update();

    // read phase
    std::string vb1{};
    std::string vb2{};
    ASSERT_EQ(search_key(s1, st, x, vb1), Status::OK);
    ASSERT_EQ(search_key(s2, st, y, vb2), Status::OK);
    std::size_t v1{};
    std::size_t v2{};
    memcpy(&v1, vb1.data(), sizeof(v1));
    ++v1;
    memcpy(&v2, vb2.data(), sizeof(v2));
    ++v2;
    std::string v1_view{reinterpret_cast<char*>(&v1), sizeof(v1)}; // NOLINT
    std::string v2_view{reinterpret_cast<char*>(&v2), sizeof(v2)}; // NOLINT
    ASSERT_EQ(upsert(s1, st, y, v1_view), Status::OK);
    ASSERT_EQ(upsert(s2, st, x, v2_view), Status::OK);

    // commit phase
    ASSERT_EQ(commit(s1), Status::OK);
    ASSERT_EQ(commit(s2), Status::ERR_CC); // s2 will break s1's read


    ASSERT_EQ(leave(s1), Status::OK);
    ASSERT_EQ(leave(s2), Status::OK);
}

} // namespace shirakami::testing
