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
        log_dir.append("/build/test_log/concurrent_update_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

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
#ifdef CPR
            while (Status::OK != search_key(s, storage, k, &t)) {
                ;
            }
#else
            ASSERT_EQ(Status::OK, search_key(s, storage, k, &t));
#endif
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
#ifdef CPR
            while (Status::OK != search_key(s, storage, k, &tuple)) {
                ;
            }
#else
            ASSERT_EQ(Status::OK, search_key(s, storage, k, &tuple));
#endif
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

} // namespace shirakami::testing