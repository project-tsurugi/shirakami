
#include <array>
#include <mutex>

#include "storage.h"

#include "concurrency_control/wp/include/wp.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class wp_storage_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-wp_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(wp_storage_test, simple) { // NOLINT
                                  // check the metadata is updated correctly.
    ASSERT_EQ(wp::get_page_set_meta_storage(), storage::wp_meta_storage);
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage(st));
    std::vector<Storage> rt{};
    ASSERT_EQ(Status::OK, list_storage(rt));
    // check result of list_storage remove about wp meta storage.
    ASSERT_EQ(1, rt.size());
    // check st num starts from 0
    ASSERT_EQ(0, rt.at(0));
    std::vector<std::pair<std::string, yakushima::tree_instance*>> rec;
    yakushima::list_storages(rec);
    // check result of list_storages from point of view of yakushima includes 2
    // storage (st and wp_meta_storage).
    ASSERT_EQ(rec.size(), 2);
}

} // namespace shirakami::testing
