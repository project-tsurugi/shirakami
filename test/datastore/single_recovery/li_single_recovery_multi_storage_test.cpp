
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"
#include "clock.h"
#include "storage.h"
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

// regression test scenario causing "unreachable path" after recovery
class li_single_recovery_multi_storage_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-data_store-"
                                  "li_single_recovery_multi_storage_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google; // NOLINT
};

void create_storages_and_upsert_one_record(int num) {
    // prepare
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    std::string name{"T" + std::to_string(num)};
    Storage t0{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, get_storage(name, t0));
    storage_option opt{static_cast<std::size_t>(100 + num), "P"}; // NOLINT
    ASSERT_EQ(Status::OK, create_storage(name, t0, opt));
    std::cerr << "t0 : " << t0 << std::endl;

    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, tx_begin(s));
    // data creation
    ASSERT_EQ(Status::OK, upsert(s, t0, "a", "a")); // (*1)
    ASSERT_EQ(Status::OK, commit(s));               // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(li_single_recovery_multi_storage_test,    // NOLINT
       check_storage_operation_after_recovery) { // NOLINT
    // start
    std::string log_dir{};
    int tid = syscall(SYS_gettid); // NOLINT
    std::uint64_t tsc = rdtsc();
    log_dir =
            "/tmp/shirakami-" + std::to_string(tid) + "-" + std::to_string(tsc);
    init({database_options::open_mode::CREATE, log_dir}); // NOLINT
    create_storages_and_upsert_one_record(0);
    create_storages_and_upsert_one_record(1);

    fin(false);

    // re-start
    init({database_options::open_mode::RESTORE, log_dir}); // NOLINT

    Storage t0{};
    ASSERT_EQ(Status::OK, get_storage("T0", t0));
    std::cerr << "t0 : " << t0 << std::endl;
    storage_option opt{};
    ASSERT_EQ(Status::OK, storage_get_options(t0, opt));
    EXPECT_EQ(opt.id(), 100);
    EXPECT_EQ(opt.payload(), "P");

    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::string vb{};
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, search_key(s, t0, "a", vb));
    ASSERT_EQ("a", vb);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
    fin();
}

} // namespace shirakami::testing