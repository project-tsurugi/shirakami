#include <bitset>
#include <future>

#include "gtest/gtest.h"

#include "concurrency_control/silo/include/epoch.h"
#include "concurrency_control/silo/include/record.h"
#include "concurrency_control/silo/include/snapshot_manager.h"

#include "clock.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

namespace shirakami::testing {

using namespace shirakami;

class simple_update : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/test_log/simple_update_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(simple_update, update_twice_for_creating_snap) { // NOLINT
    Storage storage{};
    register_storage(storage);
    std::string k("aaa");  // NOLINT
    std::string v("aaa");  // NOLINT
    std::string v2("bbb"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, insert(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
    epoch::epoch_t ce = epoch::kGlobalEpoch.load(std::memory_order_acquire);
    while (snapshot_manager::get_snap_epoch(ce) == snapshot_manager::get_snap_epoch(epoch::kGlobalEpoch.load(std::memory_order_acquire))) {
        sleepMs(1);
    }
    // change snap epoch
    ASSERT_EQ(Status::OK, enter(s));
    // it must create snap version
    ASSERT_EQ(Status::OK, update(s, storage, k, v2));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
    Record* rec_ptr{*std::get<0>(yakushima::get<Record*>({reinterpret_cast<char*>(&storage), sizeof(storage)}, k))}; // NOLINT
    ASSERT_NE(rec_ptr, nullptr);
    ASSERT_EQ(rec_ptr->get_tuple().get_value(), std::string_view(v2));
    ASSERT_EQ(rec_ptr->get_snap_ptr()->get_tuple().get_value(), std::string_view(v));
}

} // namespace shirakami::testing
