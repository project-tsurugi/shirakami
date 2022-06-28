
#include <mutex>

#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

class short_delete_insert_same_tx_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-short_tx-"
                "delete_insert/short_delete_insert_same_tx_test");
        FLAGS_stderrthreshold = 0; // output more than INFO
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

    static Storage& get_storage() { return storage_; }

private:
    static inline std::once_flag init_google_; // NOLINT
    static inline Storage storage_;            // NOLINT
};

TEST_F(short_delete_insert_same_tx_test, delete_insert) { // NOLINT
    register_storage(get_storage());
    std::string k("testing"); // NOLINT
    std::string v("bbb");     // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, get_storage(), k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, get_storage(), k));
    ASSERT_EQ(Status::OK, insert(s, get_storage(), k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(short_delete_insert_same_tx_test, delete_insert_delete) { // NOLINT
    register_storage(get_storage());
    std::string k("testing"); // NOLINT
    std::string v("v");       // NOLINT
    std::string iv("iv");     // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, get_storage(), k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, get_storage(), k));
    ASSERT_EQ(Status::OK, insert(s, get_storage(), k, iv));
    ASSERT_EQ(Status::WARN_CANCEL_PREVIOUS_UPDATE,
              delete_record(s, get_storage(), k));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Record* rec_ptr{};
    ASSERT_EQ(Status::OK, get<Record>(get_storage(), k, rec_ptr));
    std::string val{};
    rec_ptr->get_value(val);
    ASSERT_EQ(val, v);
}

TEST_F(short_delete_insert_same_tx_test, insert_delete) { // NOLINT
    Storage storage{};
    register_storage(storage);
    std::string k("k"); // NOLINT
    std::string v("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::WARN_CANCEL_PREVIOUS_INSERT,
              delete_record(s, storage, k));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // verify
    std::string vb{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, storage, k, vb));

    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(short_delete_insert_same_tx_test, insert_delete_insert) { // NOLINT
    Storage storage{};
    register_storage(storage);
    std::string k("k"); // NOLINT
    std::string v("v"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::WARN_CANCEL_PREVIOUS_INSERT,
              delete_record(s, storage, k));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
}

} // namespace shirakami::testing
