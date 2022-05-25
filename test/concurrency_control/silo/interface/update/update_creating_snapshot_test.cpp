
#include <mutex>

#include "concurrency_control/silo/include/epoch.h"
#include "concurrency_control/silo/include/record.h"
#include "concurrency_control/silo/include/snapshot_manager.h"

#include "index/yakushima/include/interface.h"

#include "clock.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

namespace shirakami::testing {

using namespace shirakami;

class simple_update : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-common-"
                                  "update-c_simple_update_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_;   // NOLINT
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
    epoch::epoch_t ce = epoch::get_global_epoch();
    while (snapshot_manager::get_snap_epoch(ce) ==
           snapshot_manager::get_snap_epoch(epoch::get_global_epoch())) {
        sleepMs(1);
    }
    // change snap epoch
    ASSERT_EQ(Status::OK, enter(s));
    // it must create snap version
    ASSERT_EQ(Status::OK, update(s, storage, k, v2));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
    Record* rec_ptr{};
    ASSERT_EQ(Status::OK, get<Record>(storage, k, rec_ptr));
    std::string val{};
    rec_ptr->get_tuple().get_value(val);
    ASSERT_EQ(val, std::string_view(v2));
    rec_ptr->get_snap_ptr()->get_tuple().get_value(val);
    ASSERT_EQ(val, std::string_view(v));
}

} // namespace shirakami::testing
