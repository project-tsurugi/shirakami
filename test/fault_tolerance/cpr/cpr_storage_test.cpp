
#include <xmmintrin.h> // NOLINT
// It is used, but clang-tidy warn.

#include <thread>
#include <vector>

#include "storage.h"

#include "concurrency_control/silo/include/session.h"

#include "fault_tolerance/include/log.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class cpr_storage_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-fault_tolerance-cpr-cpr_storage_test");
        FLAGS_stderrthreshold = 0;
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/build/cpr_storage_test");
    }

    void SetUp() override { std::call_once(init_google_, call_once_f); }

    void TearDown() override {}

    // getter
    static std::string& get_log_dir() { return log_dir_; }

    static std::vector<Storage>& get_registered_storage() {
        return registered_storage_;
    }

    static std::vector<Storage>& get_removed_storage() { return removed_storage_; }

private:
    static inline std::once_flag init_google_;              // NOLINT
    static inline std::string log_dir_{};                   // NOLINT
    static inline std::vector<Storage> registered_storage_; // NOLINT
    static inline std::vector<Storage> removed_storage_;    // NOLINT
};

TEST_F(cpr_storage_test, cpr_action_against_null_db) { // NOLINT
    init(false, get_log_dir());                        // NOLINT
    ASSERT_EQ(boost::filesystem::exists(cpr::get_checkpoint_path()),
              false); // null db has no checkpoint.
    Token token{};
    ASSERT_EQ(enter(token), Status::OK);
    std::string k("a"); // NOLINT
    Storage storage{};
    Storage storage_2{};
    Storage storage_3{};
    ASSERT_EQ(Status::OK, register_storage(storage));
    ASSERT_EQ(Status::OK, register_storage(storage_2));
    ASSERT_EQ(Status::OK, register_storage(storage_3));
    get_registered_storage().emplace_back(storage);
    get_registered_storage().emplace_back(storage_2);
    get_registered_storage().emplace_back(storage_3);
    ASSERT_EQ(Status::OK, delete_storage(storage_2));
    get_removed_storage().emplace_back(storage_2);
    ASSERT_EQ(upsert(token, storage, k, k), Status::OK);
    ASSERT_EQ(upsert(token, storage_3, k, k), Status::OK);
    auto* ti = static_cast<session*>(token);
    std::size_t sst_num{};
    if (ti->get_phase() == cpr::phase::REST) {
        sst_num = ti->get_version();
    } else {
        sst_num = ti->get_version() + 1;
    }
    ASSERT_EQ(commit(token), Status::OK); // NOLINT
    cpr::wait_next_checkpoint();
    ASSERT_EQ(boost::filesystem::exists(get_log_dir() + "/sst" +
                                        std::to_string(sst_num)),
              true);
    ASSERT_EQ(leave(token), Status::OK);
    ASSERT_EQ(storage::get_strg_ctr(), get_registered_storage().back() + 1);
    ASSERT_EQ(storage::get_reuse_num().size(), 1);
    ASSERT_EQ(get_removed_storage().size(), 1);
    ASSERT_EQ(storage::get_reuse_num(), get_removed_storage());
    fin(false);
}

TEST_F(cpr_storage_test, check_storage_meta_after_cpr_recovery) { // NOLINT
    init(true, get_log_dir());                                    // NOLINT
    ASSERT_EQ(storage::get_strg_ctr(), get_registered_storage().back() + 1);
    ASSERT_EQ(storage::get_reuse_num().size(), 1);
    ASSERT_EQ(get_removed_storage().size(), 1);
    ASSERT_EQ(storage::get_reuse_num(), get_removed_storage());
    fin(false); // NOLINT
}

} // namespace shirakami::testing
