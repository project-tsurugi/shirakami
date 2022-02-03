
#include <mutex>

#include "concurrency_control/silo/include/record.h"
#include "concurrency_control/silo/include/tuple_local.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"


namespace shirakami::testing {

using namespace shirakami;

class delete_insert : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "delete_insert-delete_insert_test");
        FLAGS_stderrthreshold = 0;        // output more than INFO
        log_dir_ = MAC2STR(PROJECT_ROOT); // NOLINT
        log_dir_.append("/tmp/"
                        "shirakami-test-concurrency_control-silo-delete_insert-"
                        "delete_insert_test_log");
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(false, log_dir_); // NOLINT
    }

    void TearDown() override { fin(); }

    static Storage& get_storage() { return storage_; }

private:
    static inline std::once_flag init_google_;
    static inline std::string log_dir_; // NOLINT
    static inline Storage storage_;
};

TEST_F(delete_insert, delete_insert) { // NOLINT
    register_storage(get_storage());
    std::string k("testing"); // NOLINT
    std::string v("bbb");     // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, get_storage(), k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, get_storage(), k));
    ASSERT_EQ(Status::WARN_WRITE_TO_LOCAL_WRITE,
              insert(s, get_storage(), k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(delete_insert, delete_insert_delete) { // NOLINT
    register_storage(get_storage());
    std::string k("testing"); // NOLINT
    std::string v("v");       // NOLINT
    std::string iv("iv");     // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, get_storage(), k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, delete_record(s, get_storage(), k));
    ASSERT_EQ(Status::WARN_WRITE_TO_LOCAL_WRITE,
              insert(s, get_storage(), k, iv));
    ASSERT_EQ(Status::WARN_CANCEL_PREVIOUS_OPERATION,
              delete_record(s, get_storage(), k));
    ASSERT_EQ(Status::OK, commit(s));                                 // NOLINT
    std::string_view st_view{reinterpret_cast<char*>(&get_storage()), // NOLINT
                             sizeof(get_storage())};
    Record** rec_d_ptr{std::get<0>(yakushima::get<Record*>(st_view, k))};
    ASSERT_NE(rec_d_ptr, nullptr);
    Record* rec_ptr{*rec_d_ptr};
    ASSERT_NE(rec_ptr, nullptr);
    std::string val{};
    rec_ptr->get_tuple().get_value(val);
    ASSERT_EQ(val, v);
}

} // namespace shirakami::testing
