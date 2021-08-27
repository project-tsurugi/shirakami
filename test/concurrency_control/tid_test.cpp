
#include "gtest/gtest.h"

#ifdef WP

#include "concurrency_control/wp/include/tid.h"

#else

#include "concurrency_control/silo/include/tid.h"

#endif
namespace shirakami::testing {

using namespace shirakami;

class tid_test : public ::testing::Test { // NOLINT
public:
    void SetUp() final {}
    void TearDown() final {}
};

TEST_F(tid_test, lock) { // NOLINT
    tid_word tid{};
    tid.lock();
    ASSERT_EQ(tid.get_lock(), true);
    tid.unlock();
    ASSERT_EQ(tid.get_lock(), false);
}

TEST_F(tid_test, latest) { // NOLINT
    tid_word tid{};
    ASSERT_EQ(tid.get_latest(), false);
    tid.set_latest(true);
    ASSERT_EQ(tid.get_latest(), true);
    tid.set_latest(false);
    ASSERT_EQ(tid.get_latest(), false);
}

TEST_F(tid_test, absent) { // NOLINT
    tid_word tid{};
    ASSERT_EQ(tid.get_absent(), false);
    tid.set_absent(true);
    ASSERT_EQ(tid.get_absent(), true);
    tid.set_absent(false);
    ASSERT_EQ(tid.get_absent(), false);
}

TEST_F(tid_test, tid) { // NOLINT
    tid_word tid{};
    ASSERT_EQ(tid.get_tid(), 0);
    tid.set_tid(1);
    ASSERT_EQ(tid.get_tid(), 1);
    tid.set_tid(2);
    ASSERT_EQ(tid.get_tid(), 2);
}

TEST_F(tid_test, epoch) { // NOLINT
    tid_word tid{};
    ASSERT_EQ(tid.get_epoch(), 0);
    tid.set_epoch(1);
    ASSERT_EQ(tid.get_epoch(), 1);
    tid.set_epoch(2);
    ASSERT_EQ(tid.get_epoch(), 2);
}

TEST_F(tid_test, compare) { // NOLINT
    // check the alignment of union
    tid_word tid1{};
    tid_word tid2{};
    tid1.set_epoch(1);
    tid1.set_tid(0);
    tid2.set_epoch(0);
    tid2.set_tid(1);
    ASSERT_EQ(tid1 > tid2, true);
    ASSERT_EQ(tid1 < tid2, false);
    ASSERT_EQ(tid1 == tid2, false);
    tid1.set_epoch(1);
    tid1.set_tid(1);
    tid2.set_epoch(0);
    tid2.set_tid(1);
    ASSERT_EQ(tid1 > tid2, true);
    ASSERT_EQ(tid1 < tid2, false);
    ASSERT_EQ(tid1 == tid2, false);
    tid1.set_epoch(0);
    tid1.set_tid(0);
    tid2.set_epoch(0);
    tid2.set_tid(0);
    ASSERT_EQ(tid1 > tid2, false);
    ASSERT_EQ(tid1 < tid2, false);
    ASSERT_EQ(tid1 == tid2, true);
    tid1.set_epoch(1);
    tid1.set_tid(1);
    tid2.set_epoch(1);
    tid2.set_tid(1);
    ASSERT_EQ(tid1 > tid2, false);
    ASSERT_EQ(tid1 < tid2, false);
    ASSERT_EQ(tid1 == tid2, true);
}

TEST_F(tid_test, multi_thread_lock) { // NOLINT
    tid_word tid;
    std::size_t counter{0};
    std::atomic<bool> ready_register{false};
    std::vector<std::thread> th_vc;
    th_vc.reserve(std::thread::hardware_concurrency());

    auto work = [&tid, &counter, &ready_register]() {
        while (!ready_register.load(std::memory_order_acquire)) _mm_pause();
        tid.lock();
        ASSERT_EQ(true, tid.get_lock());
        ++counter;
        tid.unlock();
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
