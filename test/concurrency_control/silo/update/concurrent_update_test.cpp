#include <bitset>
#include <future>

#include <glog/logging.h>

#include "gtest/gtest.h"

#include "concurrency_control/silo/include/epoch.h"
#include "concurrency_control/silo/include/record.h"
#include "concurrency_control/silo/include/snapshot_manager.h"

#include "clock.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

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
    Storage storage{};
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
            std::string vb{};
            ASSERT_EQ(Status::OK, search_key(s, storage, k, vb));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK, leave(s));
        }

        static void run(Storage storage, bool& rc) {
            std::string k("aa"); // NOLINT
            std::int64_t v{0};
            Token s{};
            ASSERT_EQ(Status::OK, enter(s));
            std::string vb{};
            Status res{search_key(s, storage, k, vb)};
            while (res == Status::WARN_CONCURRENT_UPDATE) {
                res = search_key(s, storage, k, vb);
            }
            if (res != Status::OK) { LOG(FATAL) << "fatal error"; }
            ASSERT_NE("", vb);
            v = *reinterpret_cast<std::int64_t*>( // NOLINT
                    const_cast<char*>(vb.data()));
            v++;
            std::this_thread::sleep_for(
                    std::chrono::milliseconds(100)); // NOLINT
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
            std::string vb{};
            ASSERT_EQ(Status::OK, search_key(s, storage, k, vb));
            ASSERT_NE("", vb);
            std::int64_t v{*reinterpret_cast<std::int64_t*>( // NOLINT
                    const_cast<char*>(vb.data()))};
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
