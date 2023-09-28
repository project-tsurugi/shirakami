
#include <mutex>
#include <thread>

// test/include
#include "test_tool.h"

// shirakami/
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "index/yakushima/include/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::testing {

class short_delete_short_search : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-short_tx-"
                                  "delete_scan-short_delete_scan_phantom_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(short_delete_short_search, delete_cant_cause_phantom) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, enter(s2));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(upsert(s, st, "a", ""), Status::OK);
    ASSERT_EQ(upsert(s, st, "b", ""), Status::OK);
    ASSERT_EQ(upsert(s, st, "c", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "a", scan_endpoint::INCLUSIVE, "b",
                                    scan_endpoint::INCLUSIVE, hd));
    std::string vb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, vb));
    ASSERT_EQ(Status::OK, next(s, hd));
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, vb));
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));

    // interrupt delete tx
    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, delete_record(s2, st, "c"));
    ASSERT_EQ(Status::OK, commit(s2)); // NOLINT
    // sleep so much considering gc

    // delete can't cause phantom
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, leave(s2));
}

TEST_F(short_delete_short_search,             // NOLINT
       delete_cant_cause_node_modification) { // NOLINT
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(upsert(s, st, "a", ""), Status::OK);
    ASSERT_EQ(upsert(s, st, "b", ""), Status::OK);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // scan and get node version
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "a", scan_endpoint::INCLUSIVE, "b",
                                    scan_endpoint::INCLUSIVE, hd));
    std::string vb{};
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, vb));
    auto* ti = static_cast<session*>(s);
    // node must be 1
    ASSERT_EQ(ti->get_node_set().size(), 1);
    yakushima::node_version64_body version = ti->get_node_set().front().first;
    // it can use after tx.
    yakushima::node_version64* version_source_ptr =
            ti->get_node_set().front().second;
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // delete b
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, delete_record(s, st, "b"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // wait for gc
    Record* rec_ptr{};
    while (get<Record>(st, "b", rec_ptr) != Status::WARN_NOT_FOUND) {
        _mm_pause();
    }
    // unhooked

    yakushima::node_version64_body version_after_unhooked =
            version_source_ptr->get_stable_version();
    // it must not be changed.
    ASSERT_EQ(version, version_after_unhooked);

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing