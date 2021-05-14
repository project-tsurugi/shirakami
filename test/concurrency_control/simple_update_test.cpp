#include <bitset>
#include <future>

#include "gtest/gtest.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/record.h"
#include "concurrency_control/include/snapshot_manager.h"

#include "clock.h"
#include "logger.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

using namespace shirakami::logger;

namespace shirakami::testing {

using namespace shirakami;

class simple_update : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/simple_update_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(simple_update, update) { // NOLINT
    Storage storage;
    register_storage(storage);
    std::string k("aaa");  // NOLINT
    std::string v("aaa");  // NOLINT
    std::string v2("bbb"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::WARN_NOT_FOUND, update(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Tuple* tuple{};
    ASSERT_EQ(Status::OK, search_key(s, storage, k, &tuple));
    ASSERT_EQ(
            memcmp(tuple->get_value().data(), v.data(), tuple->get_value().size()),
            0);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, update(s, storage, k, v2));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, search_key(s, storage, k, &tuple));
    ASSERT_EQ(memcmp(tuple->get_value().data(), v2.data(), v2.size()), 0);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_update, concurrent_updates) { // NOLINT
    Storage storage;
    register_storage(storage);
    struct S {
        static void prepare(Storage storage) {
            std::string k("aa"); // NOLINT
            std::int64_t v{0};
            Token s{};
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK, insert(s, storage, k,
                                         {reinterpret_cast<char*>(&v), // NOLINT
                                          sizeof(std::int64_t)}));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            Tuple* t{};
            ASSERT_EQ(Status::OK, search_key(s, storage, k, &t));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }

        static void run(Storage storage, bool& rc) {
            std::string k("aa"); // NOLINT
            std::int64_t v{0};
            Token s{};
            ASSERT_EQ(Status::OK, enter(s));
            Tuple* t{};
            Status res{search_key(s, storage, k, &t)};
            while (res == Status::WARN_CONCURRENT_UPDATE) {
                res = search_key(s, storage, k, &t);
            }
            if (res != Status::OK) {
                shirakami_logger->debug("fatal error");
                exit(1);
            }
            ASSERT_NE(nullptr, t);
            v = *reinterpret_cast<std::int64_t*>(const_cast<char*>(t->get_value().data())); // NOLINT
            v++;
            std::this_thread::sleep_for(std::chrono::milliseconds(100)); // NOLINT
            ASSERT_EQ(Status::OK, update(s, storage, k,
                                         {reinterpret_cast<char*>(&v), // NOLINT
                                          sizeof(std::int64_t)}));
            rc = (Status::OK == commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }

        static void verify(Storage storage) {
            std::string k("aa"); // NOLINT
            Token s{};
            ASSERT_EQ(Status::OK, enter(s));
            Tuple* tuple{};
            ASSERT_EQ(Status::OK, search_key(s, storage, k, &tuple));
            ASSERT_NE(nullptr, tuple);
            std::int64_t v{*reinterpret_cast<std::int64_t*>( // NOLINT
                    const_cast<char*>(tuple->get_value().data()))};
            ASSERT_EQ(10, v);
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }
    };

    S::prepare(storage);
    auto r1 = std::async(std::launch::async, [&] {
        for (int i = 0; i < 5; ++i) { // NOLINT
            bool rc = false;
            S::run(storage, rc);
            if (!rc) {
                --i;
                continue;
            }
        }
    });
    for (int i = 0; i < 5; ++i) { // NOLINT
        bool rc = false;
        S::run(storage, rc);
        if (!rc) {
            --i;
            continue;
        }
    }
    r1.wait();
    S::verify(storage);
}

TEST_F(simple_update, update_twice_for_creating_snap) { // NOLINT
    Storage storage;
    register_storage(storage);
    std::string k("aaa");  // NOLINT
    std::string v("aaa");  // NOLINT
    std::string v2("bbb"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
    epoch::epoch_t ce = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    while (snapshot_manager::get_snap_epoch(ce) == snapshot_manager::get_snap_epoch(epoch::kGlobalEpoch.load(std::memory_order_acquire))) {
        sleepMs(1);
    }
    // change snap epoch
    ASSERT_EQ(Status::OK, enter(s));
    // it must create snap version
    ASSERT_EQ(Status::OK, update(s, storage, k, v2));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
    Record* rec_ptr{*std::get<0>(yakushima::get<Record*>({reinterpret_cast<char*>(&storage), sizeof(storage)}, k))}; // NOLINT
    ASSERT_NE(rec_ptr, nullptr);
    ASSERT_EQ(rec_ptr->get_tuple().get_value(), std::string_view(v2));
    ASSERT_EQ(rec_ptr->get_snap_ptr()->get_tuple().get_value(), std::string_view(v));
}

} // namespace shirakami::testing
