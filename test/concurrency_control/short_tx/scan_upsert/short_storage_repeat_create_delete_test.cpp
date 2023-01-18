
#include <mutex>

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class storage_scan_upsert_phantom_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-silo-storage-storage_storage_scan_upsert_phantom_test");
        FLAGS_stderrthreshold = 0;
    }
    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

Status check_rc(Status rc, Token s) {
    if(rc != Status::OK) {
        auto info = transaction_result_info(s);
        LOG(INFO) << "reason: " << info->get_reason_code();
    }
    return rc;
}

// regression testcase - this scenario caused phantom
TEST_F(storage_scan_upsert_phantom_test, phantom) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("system", st));
    for(std::size_t i=0; i < 30; ++i) {
        Token s{};
        ASSERT_EQ(Status::OK, enter(s));
        ScanHandle handle{};
        std::vector<std::size_t> def_ids{};
        if(Status::OK == open_scan(s, st, "", scan_endpoint::INF, "",
                scan_endpoint::INF, handle)) {
            do {
                std::string sb{};
                ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
                def_ids.emplace_back(std::stoul(sb));
                ASSERT_EQ(Status::OK, read_value_from_scan(s, handle, sb));
            } while (next(s, handle) == Status::OK);

            ASSERT_EQ(Status::OK, close_scan(s, handle));
        }

        for(auto&& n : def_ids) {
            ASSERT_EQ(Status::OK, upsert(s, st, std::to_string(n), "value"));
        }
        ASSERT_EQ(Status::OK, upsert(s, st, std::to_string(i), "value"));
        ASSERT_EQ(Status::OK, check_rc(commit(s),s)); // NOLINT
        ASSERT_EQ(Status::OK, leave(s));
        LOG(INFO) << "repeat:" << i;
    }
}

} // namespace shirakami::testing