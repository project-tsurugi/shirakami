
#include <glog/logging.h>

#include <mutex>

#include "clock.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/session.h"

#include "concurrency_control/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class garbage_collection_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-wp-"
                                  "garbage_collection_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init();
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(garbage_collection_test, key_gc_delete_by_long) { // NOLINT
    // prepare storage
    Storage st{};
    ASSERT_EQ(Status::OK, register_storage(st));

    // prepare data
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // prepare situation
    ASSERT_EQ(Status::OK, tx_begin(s, false, true, {st}));
    auto wait_change_epoch = []() {
        auto ce{epoch::get_global_epoch()};
        for (;;) {
            if (ce != epoch::get_global_epoch()) { break; }
            _mm_pause();
        }
    };
    wait_change_epoch();
    ASSERT_EQ(Status::OK, delete_record(s, st, ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    Record* rec_ptr{};
    for (;;) {
        auto rc{get<Record>(st, "", rec_ptr)};
        if (rc == Status::WARN_NOT_FOUND) { break; }
        if (rc == Status::OK) {
            _mm_pause();
        } else {
            LOG(ERROR) << "programming error";
        }
    }

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing