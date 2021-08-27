
#include <xmmintrin.h>
#include <thread>

#include "gtest/gtest.h"

#include "concurrency_control/wp/include/lock.h"

using namespace shirakami;

namespace shirakami::testing {

class lock_test : public ::testing::Test { // NOLINT
public:
    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(lock_test, basic_test) { // NOLINT
    s_mutex mtx;
    std::size_t counter{0};
    std::atomic<bool> ready_register{false};
    std::vector<std::thread> th_vc;
    th_vc.reserve(std::thread::hardware_concurrency());

    auto work = [&mtx, &counter, &ready_register]() {
        while (!ready_register.load(std::memory_order_acquire)) _mm_pause();
        mtx.lock();
        ASSERT_EQ(true, mtx.get_locked());
        ++counter;
        mtx.unlock();
    };

    for (std::size_t i = 0; i < std::thread::hardware_concurrency(); ++i) {
        th_vc.emplace_back(work);
    }
    ready_register.store(true, std::memory_order_release);

    for (auto&& elem : th_vc) {
        elem.join();
    }

    ASSERT_EQ(counter, std::thread::hardware_concurrency());
}

} // namespace shirakami::testing
