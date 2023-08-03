#include <future>
#include <thread>

#include "shirakami/interface.h"

#include "glog/logging.h"

#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class simple_update : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        init(); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(simple_update, concurrent_updates) { // NOLINT
    Storage storage{};
    create_storage("", storage);
    struct S {
        static void prepare(Storage storage) {
            std::string k("aa"); // NOLINT
            std::int64_t v{0};
            Token s{};
            ASSERT_EQ(Status::OK, enter(s));
            ASSERT_EQ(Status::OK,
                      tx_begin({s,
                                transaction_options::transaction_type::SHORT}));
            ASSERT_EQ(Status::OK, insert(s, storage, k,
                                         {reinterpret_cast<char*>(&v), // NOLINT
                                          sizeof(std::int64_t)}));
            ASSERT_EQ(Status::OK, commit(s)); // NOLINT
            ASSERT_EQ(Status::OK,
                      tx_begin({s,
                                transaction_options::transaction_type::SHORT}));
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
            ASSERT_EQ(Status::OK,
                      tx_begin({s,
                                transaction_options::transaction_type::SHORT}));
            std::string vb{};
            Status res{search_key(s, storage, k, vb)};
            while (res == Status::WARN_CONCURRENT_UPDATE) {
                res = search_key(s, storage, k, vb);
            }
            if (res != Status::OK) {
                LOG(ERROR) << log_location_prefix << "fatal error";
            }
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
            ASSERT_EQ(Status::OK,
                      tx_begin({s,
                                transaction_options::transaction_type::SHORT}));
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
            ASSERT_NO_FATAL_FAILURE(S::run(storage, rc)); // NOLINT
            if (!rc) {
                --i;
                continue;
            }
        }
    });
    for (int i = 0; i < 5; ++i) { // NOLINT
        bool rc = false;
        ASSERT_NO_FATAL_FAILURE(S::run(storage, rc)); // NOLINT
        if (!rc) {
            --i;
            continue;
        }
    }
    r1.wait();
    S::verify(storage);
}

} // namespace shirakami::testing
