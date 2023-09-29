
#include <array>
#include <mutex>

#include "test_tool.h"

#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class c_helper_tx_begin : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-common-"
                                  "helper-c_helper_tx_begin_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(c_helper_tx_begin, tx_begin_not_change_after_warn) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    auto* ti = static_cast<session*>(s);

    ASSERT_EQ(Status::OK, tx_begin({s}));
    // expected
    ASSERT_EQ(ti->get_tx_type(), transaction_options::transaction_type::SHORT);
    ASSERT_EQ(Status::WARN_ALREADY_BEGIN, tx_begin({s}));
    // must not change
    ASSERT_EQ(ti->get_tx_type(), transaction_options::transaction_type::SHORT);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ASSERT_EQ(Status::OK, tx_begin({s}));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ASSERT_EQ(Status::OK, tx_begin({s}));
    // expected
    ASSERT_EQ(ti->get_tx_type(), transaction_options::transaction_type::SHORT);
    ASSERT_EQ(Status::WARN_ALREADY_BEGIN,
              tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    // must not change
    ASSERT_EQ(ti->get_tx_type(), transaction_options::transaction_type::SHORT);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ASSERT_EQ(Status::OK, tx_begin({s}));
    // expected
    ASSERT_EQ(ti->get_tx_type(), transaction_options::transaction_type::SHORT);
    ASSERT_EQ(Status::WARN_ALREADY_BEGIN, tx_begin({s}));
    // must not change
    ASSERT_EQ(ti->get_tx_type(), transaction_options::transaction_type::SHORT);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ASSERT_EQ(Status::OK, tx_begin({s}));
    // expected
    ASSERT_EQ(ti->get_tx_type(), transaction_options::transaction_type::SHORT);
    ASSERT_EQ(Status::WARN_ALREADY_BEGIN,
              tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    // must not change
    ASSERT_EQ(ti->get_tx_type(), transaction_options::transaction_type::SHORT);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ASSERT_EQ(Status::OK, tx_begin({s}));
    // expected
    ASSERT_EQ(ti->get_tx_type(), transaction_options::transaction_type::SHORT);
    ASSERT_EQ(Status::WARN_ALREADY_BEGIN,
              tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    // must not change
    ASSERT_EQ(ti->get_tx_type(), transaction_options::transaction_type::SHORT);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(c_helper_tx_begin, check_param_tx_type_after_tx_begin) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    auto* ti = static_cast<session*>(s);
    ASSERT_EQ(Status::OK, leave(s));

    ASSERT_EQ(Status::OK, tx_begin({s}));
    ASSERT_EQ(ti->get_tx_type(), transaction_options::transaction_type::SHORT);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::READ_ONLY}));
    ASSERT_EQ(ti->get_tx_type(),
              transaction_options::transaction_type::READ_ONLY);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ASSERT_EQ(Status::OK, tx_begin({s}));
    ASSERT_EQ(ti->get_tx_type(), transaction_options::transaction_type::SHORT);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG}));
    ASSERT_EQ(ti->get_tx_type(), transaction_options::transaction_type::LONG);
    wait_epoch_update();
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
}

TEST_F(c_helper_tx_begin, short_get_tx_began_) { // NOLINT
    Storage storage{};
    create_storage("", storage);
    std::string k("k"); // NOLINT
    std::string v("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    auto* ti = static_cast<session*>(s);
    // test upsert
    ASSERT_EQ(ti->get_tx_began(), false);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, storage, k, v));
    ASSERT_EQ(ti->get_tx_began(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_tx_began(), false);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, storage, k, v));
    ASSERT_EQ(ti->get_tx_began(), true);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(ti->get_tx_began(), false);
    // test delete
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, delete_record(s, storage, k));
    ASSERT_EQ(ti->get_tx_began(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_tx_began(), false);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, delete_record(s, storage, k));
    ASSERT_EQ(ti->get_tx_began(), true);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(ti->get_tx_began(), false);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    // test insert
    while (Status::OK != insert(s, storage, k, v)) {
        // silo impl delay physical unhook 40ms
        _mm_pause();
    }
    ASSERT_EQ(ti->get_tx_began(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_tx_began(), false);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(ti->get_tx_began(), true);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(ti->get_tx_began(), false);
    // test update
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, update(s, storage, k, v));
    ASSERT_EQ(ti->get_tx_began(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_tx_began(), false);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, update(s, storage, k, v));
    ASSERT_EQ(ti->get_tx_began(), true);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(ti->get_tx_began(), false);
    // test search
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    std::string vb{};
    ASSERT_EQ(Status::OK, search_key(s, storage, k, vb));
    ASSERT_EQ(ti->get_tx_began(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_tx_began(), false);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s, storage, k, vb));
    ASSERT_EQ(ti->get_tx_began(), true);
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(ti->get_tx_began(), false);
    // test open_scan
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ScanHandle handle = {};
    ASSERT_EQ(Status::OK, open_scan(s, storage, k, scan_endpoint::INCLUSIVE, k,
                                    scan_endpoint::INCLUSIVE, handle));
    ASSERT_EQ(handle, 0);
    ASSERT_EQ(ti->get_tx_began(), true);
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(ti->get_tx_began(), false);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, open_scan(s, storage, k, scan_endpoint::INCLUSIVE, k,
                                    scan_endpoint::INCLUSIVE, handle));
    ASSERT_EQ(handle, 0);
    ASSERT_EQ(ti->get_tx_began(), true);
    ASSERT_EQ(Status::OK, commit(s));
    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
