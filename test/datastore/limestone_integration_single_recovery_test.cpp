
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "clock.h"
#include "test_tool.h"
#include "tsc.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/lpwal.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

#include "boost/filesystem.hpp"
#include "boost/foreach.hpp"

namespace shirakami::testing {

using namespace shirakami;

class limestone_integration_single_recovery_test
    : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-data_store-"
                                  "limestone_integration_single_recovery_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google; // NOLINT
};

void create_storage_and_upsert_one_record() {
    // prepare
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    Storage st{};
    ASSERT_EQ(Status::OK, create_storage(st));

    ASSERT_EQ(Status::OK, enter(s));
    // data creation
    ASSERT_EQ(Status::OK, upsert(s, st, "", "")); // (*1)
    ASSERT_EQ(Status::OK, commit(s));             // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

std::string create_log_dir_name() {
    int tid = syscall(SYS_gettid); // NOLINT
    std::uint64_t tsc = rdtsc();
    return "/tmp/shirakami-" + std::to_string(tid) + "-" + std::to_string(tsc);
}

TEST_F(limestone_integration_single_recovery_test, // NOLINT
       one_page_write_one_storage) {               // NOLINT
    // prepare
    std::string log_dir{};
    log_dir = create_log_dir_name();
    init({database_options::open_mode::CREATE, log_dir}); // NOLINT

    Storage st{};
    ASSERT_EQ(Status::OK, create_storage(st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, st, "a", "A"));
    ASSERT_EQ(Status::OK, upsert(s, st, "b", "B"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));

    fin(false);
    init({database_options::open_mode::RESTORE, log_dir}); // NOLINT

    // test: storage num
    std::vector<Storage> st_list{};
    ASSERT_EQ(Status::OK, list_storage(st_list));
    ASSERT_EQ(st_list.size(), 1); // because single recovery
    ASSERT_EQ(Status::OK, enter(s));

    // test: contents
    for (auto&& each_st : st_list) {
        std::string vb{};
        ASSERT_EQ(Status::OK, search_key(s, each_st, "a", vb));
        ASSERT_EQ(vb, "A");
        ASSERT_EQ(Status::OK, search_key(s, each_st, "b", vb));
        ASSERT_EQ(vb, "B");
        ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    }

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
    fin(false);
}

TEST_F(limestone_integration_single_recovery_test, // NOLINT
       complicated_test) {                         // NOLINT
    std::string log_dir{};
    log_dir = create_log_dir_name();

    init({database_options::open_mode::CREATE_OR_RESTORE, log_dir}); // NOLINT
    std::vector<Storage> st_list{};
    ASSERT_EQ(Status::OK, list_storage(st_list));
    ASSERT_EQ(st_list.size(), 0);
    Storage st1{};
    ASSERT_EQ(Status::OK, create_storage(st1));
    ASSERT_EQ(Status::OK, list_storage(st_list));
    ASSERT_EQ(st_list.size(), 1);

    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT, {}}));
    std::string buf{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, st1, "\x00CHAR_TAB", buf));
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT, {}}));
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, st1, "\x00CHAR_TAB", buf));
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(Status::OK, leave(s));
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT, {}}));
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, st1, "\x00CHAR_TAB", buf));
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(Status::OK, leave(s));

    Storage st2{};
    ASSERT_EQ(Status::OK, create_storage(st2));
    ASSERT_EQ(Status::OK, list_storage(st_list));
    ASSERT_EQ(st_list.size(), 2);

    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT, {}}));
    ASSERT_EQ(Status::OK, upsert(s, st1, "\x00CHAR_TAB",
                                 "\x00\x00\x00\x00\x01\x00\x00\x00"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT, {}}));
    ASSERT_EQ(Status::OK, search_key(s, st1, "\x00CHAR_TAB", buf));
    ASSERT_EQ(Status::OK, abort(s));
    ASSERT_EQ(Status::OK, leave(s));

    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT, {}}));
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, st1, "\x00CUSTOMER", buf));

    fin(false);

    init({database_options::open_mode::RESTORE, log_dir}); // NOLINT
    ASSERT_EQ(Status::OK, list_storage(st_list));
    ASSERT_EQ(st_list.size(), 2);

    fin(false);
}

} // namespace shirakami::testing