
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class upsert_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-upsert_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/upsert_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(upsert_test, new_epoch_new_version) { // NOLINT
    Token s{};
    Storage st{};
    ASSERT_EQ(register_storage(st), Status::OK);
    ASSERT_EQ(Status::OK, enter(s));
    auto process = [s](Storage st, bool bt) {
        std::string k{"K"};
        std::string first_v{"v"};
        if (bt) {
            for (;;) {
                auto rc{upsert(s, st, k, first_v)};
                if (Status::OK == rc) { break; }
                ASSERT_EQ(rc, Status::WARN_PREMATURE);
                _mm_pause();
            }
        } else {
            ASSERT_EQ(upsert(s, st, k, first_v), Status::OK);
        }
        ASSERT_EQ(commit(s), Status::OK);
        epoch::epoch_t ce{epoch::get_global_epoch()};
        while (ce == epoch::get_global_epoch()) { _mm_pause(); }
        std::string second_v{"v2"};
        // Writing after the epoch has changed should be the new version.
        if (bt) {
            ASSERT_EQ(Status::OK, tx_begin(s, false, true, {st}));
            for (;;) {
                auto rc{upsert(s, st, k, second_v)};
                if (Status::OK == rc) { break; }
                ASSERT_EQ(rc, Status::WARN_PREMATURE);
                _mm_pause();
            }
        } else {
            ASSERT_EQ(upsert(s, st, k, second_v), Status::OK);
        }
        ASSERT_EQ(commit(s), Status::OK);
        std::string_view st_view{reinterpret_cast<char*>(&st), // NOLINT
                                 sizeof(st)};
        Record** rec_d_ptr{std::get<0>(yakushima::get<Record*>(st_view, k))};
        ASSERT_NE(rec_d_ptr, nullptr);
        Record* rec_ptr{*rec_d_ptr};
        ASSERT_NE(rec_ptr, nullptr);
        version* ver{rec_ptr->get_latest()};
        ASSERT_NE(ver, nullptr);
        ASSERT_EQ(ver->get_val(), second_v);
        ver = ver->get_next();
        ASSERT_NE(ver, nullptr);
        ASSERT_EQ(ver->get_val(), first_v);
    };
    // for occ
    process(st, false);
    LOG(INFO) << "clear about occ";
    // for batch
    ASSERT_EQ(Status::OK, tx_begin(s, false, true, {st}));
    process(st, true);
    LOG(INFO) << "clear about batch";
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing