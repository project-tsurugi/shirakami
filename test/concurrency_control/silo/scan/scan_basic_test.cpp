#include <bitset>

#include "tuple_local.h"
#include "gtest/gtest.h"

#include "shirakami/interface.h"

namespace shirakami::testing {

using namespace shirakami;

class simple_scan : public ::testing::Test { // NOLINT

public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/test_log/scan_basic_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(simple_scan, basic) { // NOLINT
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
#if defined(CPR)
    while (Status::OK != scan_key(s, storage, k, scan_endpoint::INCLUSIVE, k4, scan_endpoint::INCLUSIVE, records)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::INCLUSIVE, k4, scan_endpoint::INCLUSIVE, records));
#endif
    uint64_t ctr(0);
    ASSERT_EQ(records.size(), 3);
    for (auto&& itr : records) {
        if (ctr == 0) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
        } else if (ctr == 1) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
        } else if (ctr == 2) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
#if defined(CPR)
    while (Status::OK != scan_key(s, storage, k, scan_endpoint::EXCLUSIVE, k4, scan_endpoint::INCLUSIVE, records)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::EXCLUSIVE, k4, scan_endpoint::INCLUSIVE, records));
#endif
    ctr = 0;
    ASSERT_EQ(records.size(), 2);
    for (auto&& itr : records) {
        if (ctr == 0) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
        } else if (ctr == 1) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
#if defined(CPR)
    while (Status::OK != scan_key(s, storage, k, scan_endpoint::INCLUSIVE, k3, scan_endpoint::INCLUSIVE, records)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::INCLUSIVE, k3, scan_endpoint::INCLUSIVE, records));
#endif
    ctr = 0;
    ASSERT_EQ(records.size(), 3);
    for (auto&& itr : records) {
        if (ctr == 0) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
        } else if (ctr == 1) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
        } else if (ctr == 2) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
#if defined(CPR)
    while (Status::OK != scan_key(s, storage, k, scan_endpoint::INCLUSIVE, k3, scan_endpoint::EXCLUSIVE, records)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::INCLUSIVE, k3, scan_endpoint::EXCLUSIVE, records));
#endif
    ctr = 0;
    ASSERT_EQ(records.size(), 2);
    for (auto&& itr : records) {
        if (ctr == 0) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
        } else if (ctr == 1) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, scan_key(s, storage, "", scan_endpoint::INF, k3, scan_endpoint::INCLUSIVE, records));
    ctr = 0;
    ASSERT_EQ(records.size(), 5);
    for (auto&& itr : records) {
        if (ctr == 0) {
            ASSERT_EQ(itr->get_key().size(), 0);
        } else if (ctr == 1) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k6.data(), k6.size()), 0);
        } else if (ctr == 2) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
        } else if (ctr == 3) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
        } else if (ctr == 4) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
#if defined(CPR)
    while (Status::OK != scan_key(s, storage, "", scan_endpoint::INF, k6, scan_endpoint::INCLUSIVE, records)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, scan_key(s, storage, "", scan_endpoint::INF, k6, scan_endpoint::INCLUSIVE, records));
#endif
    ctr = 0;
    ASSERT_EQ(records.size(), 2);
    for (auto&& itr : records) {
        if (ctr == 0) {
            ASSERT_EQ(itr->get_key().size(), 0);
        } else if (ctr == 1) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k6.data(), k6.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
#if defined(CPR)
    while (Status::OK != scan_key(s, storage, "", scan_endpoint::INF, k6, scan_endpoint::EXCLUSIVE, records)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, scan_key(s, storage, "", scan_endpoint::INF, k6, scan_endpoint::EXCLUSIVE, records));
#endif
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
#if defined(CPR)
    while (Status::OK != scan_key(s, storage, k, scan_endpoint::INCLUSIVE, "", scan_endpoint::INF, records)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, scan_key(s, storage, k, scan_endpoint::INCLUSIVE, "", scan_endpoint::INF, records));
#endif
    ctr = 0;
    ASSERT_EQ(records.size(), 3);
    for (auto&& itr : records) {
        if (ctr == 0) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
        } else if (ctr == 1) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
        } else if (ctr == 2) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
#if defined(CPR)
    while (Status::OK != scan_key(s, storage, "", scan_endpoint::INF, "", scan_endpoint::INF, records)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, scan_key(s, storage, "", scan_endpoint::INF, "", scan_endpoint::INF, records));
#endif
    ctr = 0;
    ASSERT_EQ(records.size(), 5);
    for (auto&& itr : records) {
        if (ctr == 0) {
            ASSERT_EQ(itr->get_key().size(), 0);
        } else if (ctr == 1) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k6.data(), k6.size()), 0);
        } else if (ctr == 2) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
        } else if (ctr == 3) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
        } else if (ctr == 4) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
#if defined(CPR)
    while (Status::OK != scan_key(s, storage, "", scan_endpoint::INF, k5, scan_endpoint::INCLUSIVE, records)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, scan_key(s, storage, "", scan_endpoint::INF, k5, scan_endpoint::INCLUSIVE, records));
#endif
    ctr = 0;
    ASSERT_EQ(records.size(), 5);
    for (auto&& itr : records) {
        if (ctr == 0) {
            ASSERT_EQ(itr->get_key().size(), 0);
        } else if (ctr == 1) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k6.data(), k6.size()), 0);
        } else if (ctr == 2) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k.data(), k.size()), 0);
        } else if (ctr == 3) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k2.data(), k2.size()), 0);
        } else if (ctr == 4) {
            ASSERT_EQ(memcmp(itr->get_key().data(), k3.data(), k3.size()), 0);
        }
        ++ctr;
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
