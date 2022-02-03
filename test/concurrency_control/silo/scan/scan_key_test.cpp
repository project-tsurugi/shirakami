
#include <mutex>

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class scan_key_test : public ::testing::Test { // NOLINT

public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-silo-scan-scan_key_test");
        FLAGS_stderrthreshold = 0;        // output more than INFO
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/tmp/scan_key_test_log");
    }
    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(false, log_dir_); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
    static inline std::string log_dir_;        // NOLINT
};

TEST_F(scan_key_test, max_size_test) { // NOLINT
    Storage st{};
    register_storage(st);
    std::string k1("k1"); // NOLINT
    std::string k2("k2"); // NOLINT
    std::string k3("k3"); // NOLINT
    std::string v1("v1"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, st, k1, v1));
    ASSERT_EQ(Status::OK, insert(s, st, k2, v1));
    ASSERT_EQ(Status::OK, insert(s, st, k3, v1));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::vector<const Tuple*> recs{};
    ASSERT_EQ(Status::OK, scan_key(s, st, "", scan_endpoint::INF, "",
                                   scan_endpoint::INF, recs));
    ASSERT_EQ(recs.size(), 3);
    ASSERT_EQ(Status::OK, scan_key(s, st, "", scan_endpoint::INF, "",
                                   scan_endpoint::INF, recs, 1));
    ASSERT_EQ(recs.size(), 1);
    ASSERT_EQ(Status::OK, scan_key(s, st, "", scan_endpoint::INF, "",
                                   scan_endpoint::INF, recs, 2));
    ASSERT_EQ(recs.size(), 2);
    ASSERT_EQ(Status::OK, scan_key(s, st, "", scan_endpoint::INF, "",
                                   scan_endpoint::INF, recs, 3));
    ASSERT_EQ(recs.size(), 3);
    ASSERT_EQ(Status::OK, scan_key(s, st, "", scan_endpoint::INF, "",
                                   scan_endpoint::INF, recs, 4));
    ASSERT_EQ(recs.size(), 3);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(scan_key_test, basic) { // NOLINT
    Storage storage{};
    register_storage(storage);
    std::string k("aaa");   // NOLINT
    std::string k2("aab");  // NOLINT
    std::string k3("aac");  // NOLINT
    std::string k4("aad");  // NOLINT
    std::string k5("aadd"); // NOLINT
    std::string k6("aa");   // NOLINT
    std::string v("bbb");   // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, "", v));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, insert(s, storage, k2, v));
    ASSERT_EQ(Status::OK, insert(s, storage, k3, v));
    ASSERT_EQ(Status::OK, insert(s, storage, k6, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    std::vector<const Tuple*> records{};
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::INCLUSIVE, k4,
                                   scan_endpoint::INCLUSIVE, records));
    uint64_t ctr(0);
    ASSERT_EQ(records.size(), 3);
    for (auto&& itr : records) {
        std::string key{};
        if (ctr == 0) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k.data(), k.size()), 0);
        } else if (ctr == 1) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k2.data(), k2.size()), 0);
        } else if (ctr == 2) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::EXCLUSIVE, k4,
                                   scan_endpoint::INCLUSIVE, records));
    ctr = 0;
    ASSERT_EQ(records.size(), 2);
    for (auto&& itr : records) {
        std::string key{};
        if (ctr == 0) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k2.data(), k2.size()), 0);
        } else if (ctr == 1) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::INCLUSIVE, k3,
                                   scan_endpoint::INCLUSIVE, records));
    ctr = 0;
    ASSERT_EQ(records.size(), 3);
    for (auto&& itr : records) {
        std::string key{};
        if (ctr == 0) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k.data(), k.size()), 0);
        } else if (ctr == 1) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k2.data(), k2.size()), 0);
        } else if (ctr == 2) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::INCLUSIVE, k3,
                                   scan_endpoint::EXCLUSIVE, records));
    ctr = 0;
    ASSERT_EQ(records.size(), 2);
    for (auto&& itr : records) {
        std::string key{};
        if (ctr == 0) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k.data(), k.size()), 0);
        } else if (ctr == 1) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k2.data(), k2.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, scan_key(s, storage, "", scan_endpoint::INF, k3,
                                   scan_endpoint::INCLUSIVE, records));
    ctr = 0;
    ASSERT_EQ(records.size(), 5);
    for (auto&& itr : records) {
        std::string key{};
        if (ctr == 0) {
            itr->get_key(key);
            ASSERT_EQ(key.size(), 0);
        } else if (ctr == 1) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k6.data(), k6.size()), 0);
        } else if (ctr == 2) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k.data(), k.size()), 0);
        } else if (ctr == 3) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k2.data(), k2.size()), 0);
        } else if (ctr == 4) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, scan_key(s, storage, "", scan_endpoint::INF, k6,
                                   scan_endpoint::INCLUSIVE, records));
    ctr = 0;
    ASSERT_EQ(records.size(), 2);
    for (auto&& itr : records) {
        std::string key{};
        if (ctr == 0) {
            itr->get_key(key);
            ASSERT_EQ(key.size(), 0);
        } else if (ctr == 1) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k6.data(), k6.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, scan_key(s, storage, "", scan_endpoint::INF, k6,
                                   scan_endpoint::EXCLUSIVE, records));
    ctr = 0;
    ASSERT_EQ(records.size(), 1);
    for ([[maybe_unused]] auto&& itr : records) {
        if (ctr == 0) {
            // ASSERT_EQ(itr->get_key().data(), nullptr);
            /*
             * key which is nullptr was inserted, but itr->get_key().data() refer
             * record, so not nullptr.
             */
            ++ctr;
        }
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::INCLUSIVE, "",
                                   scan_endpoint::INF, records));
    ctr = 0;
    ASSERT_EQ(records.size(), 3);
    for (auto&& itr : records) {
        std::string key{};
        if (ctr == 0) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k.data(), k.size()), 0);
        } else if (ctr == 1) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k2.data(), k2.size()), 0);
        } else if (ctr == 2) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, scan_key(s, storage, "", scan_endpoint::INF, "",
                                   scan_endpoint::INF, records));
    ctr = 0;
    ASSERT_EQ(records.size(), 5);
    for (auto&& itr : records) {
        std::string key{};
        if (ctr == 0) {
            itr->get_key(key);
            ASSERT_EQ(key.size(), 0);
        } else if (ctr == 1) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k6.data(), k6.size()), 0);
        } else if (ctr == 2) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k.data(), k.size()), 0);
        } else if (ctr == 3) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k2.data(), k2.size()), 0);
        } else if (ctr == 4) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, scan_key(s, storage, "", scan_endpoint::INF, k5,
                                   scan_endpoint::INCLUSIVE, records));
    ctr = 0;
    ASSERT_EQ(records.size(), 5);
    for (auto&& itr : records) {
        std::string key{};
        if (ctr == 0) {
            itr->get_key(key);
            ASSERT_EQ(key.size(), 0);
        } else if (ctr == 1) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k6.data(), k6.size()), 0);
        } else if (ctr == 2) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k.data(), k.size()), 0);
        } else if (ctr == 3) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k2.data(), k2.size()), 0);
        } else if (ctr == 4) {
            itr->get_key(key);
            ASSERT_EQ(memcmp(key.data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
