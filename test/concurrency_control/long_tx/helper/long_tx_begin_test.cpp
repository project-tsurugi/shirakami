
#include <mutex>
#include <vector>

#include "test_tool.h"
#include "shirakami/interface.h"
#include "gtest/gtest.h"
#include "glog/logging.h"
#include "shirakami/api_storage.h"
#include "shirakami/binary_printer.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"
#include "shirakami/transaction_options.h"

namespace shirakami::testing {

using namespace shirakami;

class long_tx_begin_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-long_tx-"
                                  "helper-long_tx_begin_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

TEST_F(long_tx_begin_test, tx_begin_wp_existing_storage) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("1", st));
    std::vector<Storage> wp{st};
    // wp for existing storage
    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::LONG, wp}),
              Status::OK);
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_tx_begin_test, tx_begin_wp_not_existing_storage) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    std::vector<Storage> wp{1, 2, 3};
    // wp for non-existing storage
    ASSERT_EQ(tx_begin({s, transaction_options::transaction_type::LONG, wp}),
              Status::WARN_INVALID_ARGS);
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_tx_begin_test, read_area_default_constructor) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(
            Status::OK,
            tx_begin({s, transaction_options::transaction_type::LONG, {}, {}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_tx_begin_test, positive_read_area_not_existing_storage) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    Storage st{};
    ASSERT_EQ(Status::OK, tx_begin({s,
                                    transaction_options::transaction_type::LONG,
                                    {},
                                    {{st}, {}}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_tx_begin_test, negative_read_area_not_existing_storage) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    Storage st{};
    ASSERT_EQ(Status::OK, tx_begin({s,
                                    transaction_options::transaction_type::LONG,
                                    {},
                                    {{}, {st}}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_tx_begin_test, positive_read_area_existing_storage) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("1", st));
    ASSERT_EQ(Status::OK, tx_begin({s,
                                    transaction_options::transaction_type::LONG,
                                    {},
                                    {{st}, {}}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(long_tx_begin_test, negative_read_area_existing_storage) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("1", st));
    ASSERT_EQ(Status::OK, tx_begin({s,
                                    transaction_options::transaction_type::LONG,
                                    {},
                                    {{}, {st}}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
