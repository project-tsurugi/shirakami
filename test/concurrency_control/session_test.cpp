
#include <mutex>

#include "clock.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class session_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-session_test");
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

TEST_F(session_test, member_operating) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    // prepare data
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    auto* ti{static_cast<session*>(s)};
    ASSERT_EQ(ti->get_operating(), 0);
    ASSERT_EQ(Status::OK, tx_begin({s}));
    ASSERT_EQ(ti->get_operating(), 0);
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(ti->get_operating(), 0);
    ASSERT_EQ(Status::WARN_NOT_BEGIN, abort(s));
    ASSERT_EQ(ti->get_operating(), 0);
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, st, "k", ""));
    ASSERT_EQ(ti->get_operating(), 0);
    ASSERT_EQ(Status::OK, update(s, st, "k", ""));
    ASSERT_EQ(ti->get_operating(), 0);
    ASSERT_EQ(Status::OK, upsert(s, st, "k", ""));
    ASSERT_EQ(ti->get_operating(), 0);
    ASSERT_EQ(Status::WARN_CANCEL_PREVIOUS_INSERT, delete_record(s, st, "k"));
    ASSERT_EQ(ti->get_operating(), 0);
    std::string sb{};
    ASSERT_EQ(Status::OK, search_key(s, st, "", sb));
    ASSERT_EQ(ti->get_operating(), 0);
    ASSERT_EQ(Status::OK, exist_key(s, st, ""));
    ASSERT_EQ(ti->get_operating(), 0);
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    ASSERT_EQ(ti->get_operating(), 0);
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
    ASSERT_EQ(ti->get_operating(), 0);
    ASSERT_EQ(Status::OK, read_value_from_scan(s, hd, sb));
    ASSERT_EQ(ti->get_operating(), 0);
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));
    ASSERT_EQ(ti->get_operating(), 0);
    std::size_t sz{};
    ASSERT_EQ(Status::OK, scannable_total_index_size(s, hd, sz));
    ASSERT_EQ(ti->get_operating(), 0);
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(session_test, member_step_epoch_after_each_api) { // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    // prepare data
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    auto* ti{static_cast<session*>(s)};
    auto wait_change_step_epoch = [ti]() {
        auto ce{ti->get_step_epoch()};
        for (;;) {
            auto ne{ti->get_step_epoch()};
            if (ce != ne) {
                LOG(INFO) << ce << " " << ne;
                break;
            }
            _mm_pause();
        }
    };
    ASSERT_EQ(Status::OK, tx_begin({s}));
    wait_change_step_epoch();
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    wait_change_step_epoch();
    ASSERT_EQ(Status::WARN_NOT_BEGIN, abort(s));
    wait_change_step_epoch();
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, st, "k", ""));
    wait_change_step_epoch();
    ASSERT_EQ(Status::OK, update(s, st, "k", ""));
    wait_change_step_epoch();
    ASSERT_EQ(Status::OK, upsert(s, st, "k", ""));
    wait_change_step_epoch();
    ASSERT_EQ(Status::WARN_CANCEL_PREVIOUS_INSERT, delete_record(s, st, "k"));
    wait_change_step_epoch();
    std::string sb{};
    ASSERT_EQ(Status::OK, search_key(s, st, "", sb));
    wait_change_step_epoch();
    ASSERT_EQ(Status::OK, exist_key(s, st, ""));
    wait_change_step_epoch();
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    wait_change_step_epoch();
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, sb));
    wait_change_step_epoch();
    ASSERT_EQ(Status::OK, read_value_from_scan(s, hd, sb));
    wait_change_step_epoch();
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));
    wait_change_step_epoch();
    std::size_t sz{};
    ASSERT_EQ(Status::OK, scannable_total_index_size(s, hd, sz));
    wait_change_step_epoch();
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
