
#include <mutex>
#include <vector>

#include "clock.h"

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

#define ASSERT_OK(expr) ASSERT_EQ(expr, shirakami::Status::OK)

namespace shirakami::testing {

class tsurugi_issue308 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue308");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(tsurugi_issue308, 20230621_comment_kurosawa_slack) { // NOLINT
    Storage st1{};
    Storage st2{};
    ASSERT_OK(create_storage("test1", st1));
    ASSERT_OK(create_storage("test2", st2));
    Token s{};
    ASSERT_OK(enter(s));

    // data creation
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st1, "", "v1"));
    ASSERT_OK(upsert(s, st2, "", "v1"));
    ASSERT_OK(commit(s));

    // wp st1, st2. read potisitive st1, read negative st2
    ASSERT_OK(tx_begin({s,
                        transaction_options::transaction_type::LONG,
                        {st1, st2},
                        {{st1}, {st2}}}));
    wait_epoch_update();
    std::string buf{};
    ASSERT_OK(search_key(s, st1, "", buf));
    ASSERT_EQ(buf, "v1");
    ASSERT_OK(upsert(s, st1, "", "v2"));
    ASSERT_OK(upsert(s, st2, "", "v2"));
    ASSERT_OK(commit(s));

    // delete storage
    ASSERT_OK(delete_storage(st1));
    ASSERT_OK(delete_storage(st2));

    // create storage by same name
    ASSERT_OK(create_storage("test1", st1));
    ASSERT_OK(create_storage("test2", st2));

    // do same operation
    // data creation
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_OK(upsert(s, st1, "", "v1"));
    ASSERT_OK(upsert(s, st2, "", "v1"));
    ASSERT_OK(commit(s));

    // wp st1, st2. read potisitive st1, read negative st2
    ASSERT_OK(tx_begin({s,
                        transaction_options::transaction_type::LONG,
                        {st1, st2},
                        {{st1}, {st2}}}));
    wait_epoch_update();
    ASSERT_OK(search_key(s, st1, "", buf));
    ASSERT_EQ(buf, "v1");
    ASSERT_OK(upsert(s, st1, "", "v2"));
    ASSERT_OK(upsert(s, st2, "", "v2"));
    ASSERT_OK(commit(s));

    ASSERT_OK(leave(s));
}

} // namespace shirakami::testing