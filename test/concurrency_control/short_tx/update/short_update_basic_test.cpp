
#include <mutex>

#ifdef WP

#include "concurrency_control/wp/include/record.h"

#else

#include "concurrency_control/silo/include/record.h"

#endif

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class simple_update : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-common-"
                                  "update-update_basic_test");
    }
    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(simple_update, update) { // NOLINT
    Storage storage{};
    create_storage(storage);
    std::string k("aaa");  // NOLINT
    std::string v("aaa");  // NOLINT
    std::string v2("bbb"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::WARN_NOT_FOUND, update(s, storage, k, v));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(yakushima::status::OK, put<Record>(storage, k, v));
    ASSERT_EQ(Status::OK, update(s, storage, k, v2));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    Record* rec_ptr{};
    ASSERT_EQ(Status::OK, get<Record>(storage, k, rec_ptr));
    std::string vb{};
    rec_ptr->get_value(vb);
    ASSERT_EQ(vb, v2);
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
