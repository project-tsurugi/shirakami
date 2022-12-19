
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

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/lpwal.h"
#include "concurrency_control/include/record.h"
#include "concurrency_control/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

#include "boost/filesystem.hpp"
#include "boost/foreach.hpp"

namespace shirakami::testing {

using namespace shirakami;

class li_logging_callback_test
    : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-data_store-"
                "li_logging_callback_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google; // NOLINT
};

TEST_F(li_logging_callback_test, // NOLINT
       simple_check_logging_callback) {             // NOLINT
    // prepare test
    init({database_options::open_mode::CREATE}); // NOLINT
    database_set_logging_callback(
            [](std::size_t n, log_record* begin, log_record* end) {
                log_record* lrptr = begin;
                for (;;) {
                    if (lrptr == end) { break; }
                    LOG(INFO) << n << " " << lrptr->get_operation() << " "
                              << lrptr->get_key() << " " << lrptr->get_value()
                              << " " << lrptr->get_major_version() << " "
                              << lrptr->get_minor_version() << " "
                              << lrptr->get_storage_id();
                    ++lrptr; // NOLINT
                }
            });
    Storage st{};
    Storage st2{};
    Storage st3{};
    ASSERT_EQ(Status::OK, create_storage("1", st));
    ASSERT_EQ(Status::OK, create_storage("2", st2));
    ASSERT_EQ(Status::OK, create_storage("3", st3, 3));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // prepare data
    ASSERT_EQ(Status::OK, upsert(s, st, "a", "b"));
    ASSERT_EQ(Status::OK, upsert(s, st, "c", "d"));
    ASSERT_EQ(Status::OK, upsert(s, st2, "x", "y"));
    ASSERT_EQ(Status::OK, upsert(s, st2, "z", "w"));
    ASSERT_EQ(Status::OK, upsert(s, st3, "e", "f"));
    ASSERT_EQ(Status::OK, upsert(s, st3, "g", "h"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT // (*1)

    fin(false);
}

TEST_F(li_logging_callback_test,               // NOLINT
       short_tx_two_page_insert_update_delete_logging_callback) { // NOLINT
    // prepare test
    init({database_options::open_mode::CREATE}); // NOLINT
    std::atomic<std::uint64_t> count{0};
    database_set_logging_callback(
            [&count](std::size_t n, log_record* begin, log_record* end) {
                log_record* lrptr = begin;
                for (;;) {
                    if (lrptr == end) { break; }
                    LOG(INFO) << n << " " << lrptr->get_operation() << " "
                              << lrptr->get_key() << " " << lrptr->get_value()
                              << " " << lrptr->get_major_version() << " "
                              << lrptr->get_minor_version() << " "
                              << lrptr->get_storage_id();
                    ++count;
                    ++lrptr; // NOLINT
                }
            });
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st, {2}));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // insert two page
    ASSERT_EQ(Status::OK, insert(s, st, "A", "B"));
    ASSERT_EQ(Status::OK, insert(s, st, "a", "b"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // update two page
    ASSERT_EQ(Status::OK, update(s, st, "A", "C"));
    ASSERT_EQ(Status::OK, update(s, st, "a", "c"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // delete two page
    ASSERT_EQ(Status::OK, delete_record(s, st, "A"));
    ASSERT_EQ(Status::OK, delete_record(s, st, "a"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    fin(false);

    ASSERT_EQ(count, 6);
}

TEST_F(li_logging_callback_test,              // NOLINT
       long_tx_two_page_insert_update_delete_logging_callback) { // NOLINT
    // prepare test
    init({database_options::open_mode::CREATE}); // NOLINT
    std::atomic<std::uint64_t> count{0};
    database_set_logging_callback(
            [&count](std::size_t n, log_record* begin, log_record* end) {
                log_record* lrptr = begin;
                for (;;) {
                    if (lrptr == end) { break; }
                    LOG(INFO) << n << " " << lrptr->get_operation() << " "
                              << lrptr->get_key() << " " << lrptr->get_value()
                              << " " << lrptr->get_major_version() << " "
                              << lrptr->get_minor_version() << " "
                              << lrptr->get_storage_id();
                    ++count;
                    ++lrptr; // NOLINT
                }
            });
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st, {2}));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // insert two page
    transaction_options to{s,
                           transaction_options::transaction_type::LONG,
                           {st}};
    ASSERT_EQ(Status::OK, tx_begin(to));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, insert(s, st, "A", "B"));
    ASSERT_EQ(Status::OK, insert(s, st, "a", "b"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // update two page
    ASSERT_EQ(Status::OK, tx_begin(to));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, update(s, st, "A", "C"));
    ASSERT_EQ(Status::OK, update(s, st, "a", "c"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // delete two page
    ASSERT_EQ(Status::OK, tx_begin(to));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, delete_record(s, st, "A"));
    ASSERT_EQ(Status::OK, delete_record(s, st, "a"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    fin(false);

    ASSERT_EQ(count, 6);
}

} // namespace shirakami::testing