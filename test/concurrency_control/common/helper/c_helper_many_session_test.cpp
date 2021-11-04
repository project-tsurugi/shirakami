
#include <xmmintrin.h>

#include <mutex>
#include <thread>
#include <vector>

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class c_helper_many_session : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control_common-"
                                  "helper-c_helper_many_session_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/c_helper_many_session_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(c_helper_many_session, heavy_enter_leave) { // NOLINT
    auto repeat_enter_leave = []() {
        Token s{};
        constexpr std::size_t repeat_num{1000};
        for (std::size_t i = 0; i < repeat_num; ++i) {
            while (Status::OK != enter(s)) { _mm_pause(); }
            ASSERT_EQ(Status::OK, leave(s));
        }
    };

    constexpr std::size_t th_num{1000}; // NOLINT
    std::vector<std::thread> ths;
    ths.reserve(th_num);
    for (std::size_t i = 0; i < th_num; ++i) {
        ths.emplace_back(repeat_enter_leave);
    }

    for (auto&& th : ths) th.join();
}

} // namespace shirakami::testing
