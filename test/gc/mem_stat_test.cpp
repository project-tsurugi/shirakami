
#include "shirakami/interface.h"
#include "yakushima/include/kvs.h"

#include "test_tool.h"

using namespace shirakami;

namespace shirakami::testing {

class mem_stat_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-gc-mem_stat_test");
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

// this is not a test, but a sample program to display STATS-BY-GC and yakushima MEM-USAGE
TEST_F(mem_stat_test, demo) {
    Storage st_simple{};
    ASSERT_OK(create_storage("SIMPLE", st_simple));
    Storage st_empty{};
    ASSERT_OK(create_storage("EMPTY", st_empty));
    Storage st_multi_ver{};
    ASSERT_OK(create_storage("MULTI_VER", st_multi_ver));
    Storage st_long_prefix_kv{}; // long common prefix
    ASSERT_OK(create_storage("LONG_PREFIX_KV", st_long_prefix_kv));
    Storage st_long_suffix_kv{}; // long common suffix
    ASSERT_OK(create_storage("LONG_SUFFIX_KV", st_long_suffix_kv));

    Token rtx1{};
    Token rtx2{};
    Token rtx3{};
    Token occ1{};

    ASSERT_OK(enter(occ1));
    ASSERT_OK(tx_begin({occ1, transaction_options::transaction_type::SHORT}));
    std::string longstr_k(80U, 'K');
    std::string longstr_v(80U, 'V');
    for (std::size_t i = 0; i < 100U; i++) {
        std::string key = "K" + std::to_string(i);
        std::string value = "V" + std::to_string(i);
        ASSERT_OK(upsert(occ1, st_simple, key, value));
        ASSERT_OK(upsert(occ1, st_multi_ver, key, value));
        ASSERT_OK(upsert(occ1, st_long_prefix_kv, longstr_k + key, longstr_v + value));
        ASSERT_OK(upsert(occ1, st_long_suffix_kv, key + longstr_k, value + longstr_v));
    }
    ASSERT_OK(commit(occ1));
    ASSERT_OK(leave(occ1));

    // keep snapshot
    wait_epoch_update();
    ASSERT_OK(enter(rtx1));
    ASSERT_OK(tx_begin({rtx1, transaction_options::transaction_type::READ_ONLY}));
    wait_epoch_update();

    ASSERT_OK(enter(occ1));
    ASSERT_OK(tx_begin({occ1, transaction_options::transaction_type::SHORT}));
    for (std::size_t i = 0; i < 100U; i++) {
        std::string key = "K" + std::to_string(i);
        std::string value = "V" + std::to_string(i + 1);
        ASSERT_OK(upsert(occ1, st_multi_ver, key, value));
    }
    ASSERT_OK(commit(occ1));
    ASSERT_OK(leave(occ1));

    // keep snapshot
    wait_epoch_update();
    ASSERT_OK(enter(rtx2));
    ASSERT_OK(tx_begin({rtx2, transaction_options::transaction_type::READ_ONLY}));
    wait_epoch_update();

    ASSERT_OK(enter(occ1));
    ASSERT_OK(tx_begin({occ1, transaction_options::transaction_type::SHORT}));
    for (std::size_t i = 0; i < 100U; i += 2) {
        std::string key = "K" + std::to_string(i);
        ASSERT_OK(delete_record(occ1, st_multi_ver, key));
    }
    ASSERT_OK(commit(occ1));
    ASSERT_OK(leave(occ1));

    // keep snapshot
    wait_epoch_update();
    ASSERT_OK(enter(rtx3));
    ASSERT_OK(tx_begin({rtx3, transaction_options::transaction_type::READ_ONLY}));
    wait_epoch_update();

    ASSERT_OK(enter(occ1));
    ASSERT_OK(tx_begin({occ1, transaction_options::transaction_type::SHORT}));
    for (std::size_t i = 0; i < 100U; i += 5) {
        std::string key = "K" + std::to_string(i);
        std::string value = "vv" + std::to_string(i);
        ASSERT_OK(upsert(occ1, st_multi_ver, key, value));
    }
    ASSERT_OK(commit(occ1));
    ASSERT_OK(leave(occ1));
    wait_epoch_update();

    LOG(INFO) << "index setup done";
    wait_epoch_update();
    yakushima::mem_usage_display_all();

    ASSERT_OK(commit(rtx1));
    ASSERT_OK(commit(rtx2));
    ASSERT_OK(commit(rtx3));
    ASSERT_OK(leave(rtx3));
    ASSERT_OK(leave(rtx2));
    ASSERT_OK(leave(rtx1));
}

} // namespace shirakami::testing
