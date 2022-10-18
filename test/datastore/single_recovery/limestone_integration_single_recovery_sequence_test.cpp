
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "clock.h"
#include "storage.h"
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

class limestone_integration_single_recovery_sequence_test
    : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-data_store-"
                "limestone_integration_single_recovery_sequence_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google; // NOLINT
};

std::string create_log_dir_name() {
    int tid = syscall(SYS_gettid); // NOLINT
    std::uint64_t tsc = rdtsc();
    return "/tmp/shirakami-" + std::to_string(tid) + "-" + std::to_string(tsc);
}

TEST_F(limestone_integration_single_recovery_sequence_test, // NOLINT
       sequence_api_test) {                                 // NOLINT
    // prepare
    std::string log_dir{};
    log_dir = create_log_dir_name();
    init({database_options::open_mode::CREATE, log_dir}); // NOLINT

    // before shutdown
    // id1: create sequence only
    SequenceId id1{};
    ASSERT_EQ(Status::OK, create_sequence(&id1));
    // id2: create sequence and update
    SequenceId id2{};
    ASSERT_EQ(Status::OK, create_sequence(&id2));
    SequenceVersion version{2};
    SequenceValue value{3};
    Token token{};
    ASSERT_EQ(Status::OK, enter(token));
    ASSERT_EQ(Status::OK, update_sequence(token, id2, version, value));
    ASSERT_EQ(Status::OK, leave(token));
    // id3: create sequence and delete
    SequenceId id3{};
    ASSERT_EQ(Status::OK, create_sequence(&id3));
    ASSERT_EQ(Status::OK, delete_sequence(id3));

    fin(false);
    init({database_options::open_mode::RESTORE, log_dir}); // NOLINT

    // test: contents
    // id1
    SequenceVersion check_version{};
    SequenceValue check_value{};
    ASSERT_EQ(Status::OK, read_sequence(id1, &check_version, &check_value));
    ASSERT_EQ(check_version, 0);
    ASSERT_EQ(check_value, 0);
    // id2
    // read data created before recovery
    ASSERT_EQ(Status::OK, read_sequence(id2, &check_version, &check_value));
    ASSERT_EQ(check_version, 2);
    ASSERT_EQ(check_value, 3);
    // id3
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              read_sequence(id3, &check_version, &check_value));

    // test: CRUD after recovery
    // create
    SequenceId id4{};
    ASSERT_EQ(Status::OK, create_sequence(&id4));

    // wait updating result effect
    auto wait_update = []() {
        auto ce = epoch::get_global_epoch(); // current epoch
        for (;;) {
            if (lpwal::get_durable_epoch() > ce) { break; }
            _mm_pause();
        }
    };

    wait_update();
    // read data created after recovery
    ASSERT_EQ(Status::OK, read_sequence(id4, &check_version, &check_value));
    ASSERT_EQ(check_version, 0);
    ASSERT_EQ(check_value, 0);

    ASSERT_EQ(Status::OK, enter(token));
    // update data created before recovery (*1)
    version = 4; // NOLINT
    value = 5;   // NOLINT
    ASSERT_EQ(Status::OK, update_sequence(token, id2, version, value));
    // update data created after recovery (*2)
    version = 6; // NOLINT
    value = 7;   // NOLINT
    ASSERT_EQ(Status::OK, update_sequence(token, id4, version, value));
    wait_update();
    // check (*1)
    ASSERT_EQ(Status::OK, read_sequence(id2, &check_version, &check_value));
    ASSERT_EQ(check_version, 4);
    ASSERT_EQ(check_value, 5);
    // check (*2)
    ASSERT_EQ(Status::OK, read_sequence(id4, &check_version, &check_value));
    ASSERT_EQ(check_version, 6);
    ASSERT_EQ(check_value, 7);
    // return transaction handle
    ASSERT_EQ(Status::OK, leave(token));

    // delete data created before recovery
    ASSERT_EQ(Status::OK, delete_sequence(id2));
    // delete data created after recovery
    ASSERT_EQ(Status::OK, delete_sequence(id4));

    wait_update();
    // check delete data by read_sequence
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              read_sequence(id2, &check_version, &check_value));
    ASSERT_EQ(Status::WARN_NOT_FOUND,
              read_sequence(id4, &check_version, &check_value));

    // cleanup
    fin(false);
}

} // namespace shirakami::testing