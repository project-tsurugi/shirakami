
#include <xmmintrin.h>

#include <thread>
#include <vector>

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "tuple_local.h"

namespace shirakami::testing {

using namespace shirakami;

class helper_many_session : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/helper_many_session_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(helper_many_session, heavy_enter_leave) { // NOLINT
    auto repeat_enter_leave = []() {
        Token s{};
        constexpr std::size_t repeat_num{1000};
        for (std::size_t i = 0; i < repeat_num; ++i) {
            while (Status::OK != enter(s)) {
                _mm_pause();
            }
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
