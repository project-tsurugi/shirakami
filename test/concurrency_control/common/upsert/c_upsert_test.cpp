
#include <array>
#include <mutex>
#include <string_view>

#ifdef WP

#include "concurrency_control/wp/include/record.h"

#else

#include "concurrency_control/silo/include/record.h"

#endif

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class upsert_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-common-"
                                  "c_upsert_test-upsert_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/c_upsert_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(upsert_test, simple) { // NOLINT
    Token s{};
    Storage storage{};
    ASSERT_EQ(register_storage(storage), Status::OK);
    ASSERT_EQ(enter(s), Status::OK);
    std::string k{"k"};
    std::string v{"v"};
    std::string v2{"v2"};
    ASSERT_EQ(upsert(s, storage, k, v), Status::OK);
    ASSERT_EQ(commit(s), Status::OK);
    std::string_view st_view{reinterpret_cast<char*>(&storage), // NOLINT
                             sizeof(storage)};
    Record** rec_d_ptr{std::get<0>(yakushima::get<Record*>(st_view, k))};
    ASSERT_NE(rec_d_ptr, nullptr);
    Record* rec_ptr{*rec_d_ptr};
    ASSERT_NE(rec_ptr, nullptr);
    std::string key{};
    rec_ptr->get_key(key);
    ASSERT_EQ(key, k);
    ASSERT_EQ(upsert(s, storage, k, v2), Status::OK);
    ASSERT_EQ(commit(s), Status::OK);
    ASSERT_EQ(leave(s), Status::OK);
}

} // namespace shirakami::testing
