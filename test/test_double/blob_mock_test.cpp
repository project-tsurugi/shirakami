
#include "concurrency_control/include/session.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

#include "test_tool.h"
#include "doubles/limestone_double.h"

namespace shirakami::testing {

using namespace shirakami;

class blob_mock_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-test_double-blob_mock_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
    }
    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override {
        fin();
        test_double::log_channel_add_entry1::hook_func = nullptr;
        test_double::log_channel_add_entry2::hook_func = nullptr;
        test_double::datastore_switch_available_boundary_version::hook_func = nullptr;
    }

private:
    static inline std::once_flag init_google_;
};

TEST_F(blob_mock_test, insert_update_blob) {
    std::vector<std::pair<std::string, std::vector<limestone::api::blob_id_type>>> added;
    // prepare

    // test double
    test_double::log_channel_add_entry1::hook_func = [&added] (
            [[maybe_unused]] test_double::log_channel_add_entry1::orig_type orig_func,
            [[maybe_unused]] limestone::api::log_channel* this_ptr,
            [[maybe_unused]] limestone::api::storage_id_type storage_id,
            [[maybe_unused]] std::string_view key, [[maybe_unused]] std::string_view value,
            [[maybe_unused]] limestone::api::write_version_type write_version) -> void {
        VLOG(40) << "add_entry 1 storage_id:" << storage_id << " key:" << key;
        added.emplace_back(key, std::vector<limestone::api::blob_id_type>{});
    };
    test_double::log_channel_add_entry2::hook_func = [&added] (
            [[maybe_unused]] test_double::log_channel_add_entry2::orig_type orig_func,
            [[maybe_unused]] limestone::api::log_channel* this_ptr,
            [[maybe_unused]] limestone::api::storage_id_type storage_id,
            [[maybe_unused]] std::string_view key, [[maybe_unused]] std::string_view value,
            [[maybe_unused]] limestone::api::write_version_type write_version,
            [[maybe_unused]] const std::vector<limestone::api::blob_id_type>& large_objects) -> void {
        VLOG(40) << "add_entry 2 storage_id:" << storage_id << " key:" << key << shirakami_vecstring(large_objects);
        added.emplace_back(key, large_objects);
    };

    Storage st{};
    create_storage("", st); // N.B. create_storage may call add_entry
    Token s{};
    ASSERT_OK(enter(s));
    added.clear();

    // test and verify
    ASSERT_EQ(added.size(), 0);
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    const blob_id_type b1[2] = {11, 22};
    ASSERT_OK(insert(s, st, "k", "v", b1, 2));
    const blob_id_type b2[3] = {99, 88, 77};
    ASSERT_OK(update(s, st, "k", "v1", b2, 3));
    ASSERT_OK(commit(s));
    {
        ASSERT_EQ(added.size(), 1);
        auto lobs = std::get<1>(added.at(0));
        EXPECT_EQ(std::vector(std::begin(b2), std::end(b2)), lobs);
    }

    // cleanup
    ASSERT_OK(leave(s));
}

} // namespace shirakami::testing
