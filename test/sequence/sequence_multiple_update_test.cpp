/**
 * @file sequence_test.cpp
 */

#include <emmintrin.h>
#include <chrono>
#include <memory>
#include <mutex>
#include <ostream>
#include <string_view>
#include <thread>

#include "shirakami/interface.h"
#include "gtest/gtest.h"
#include "glog/logging.h"
#include "concurrency_control/include/epoch.h"
#include "shirakami/api_sequence.h"
#include "shirakami/binary_printer.h"
#include "shirakami/logging.h"
#include "shirakami/scheme.h"
#include "shirakami/transaction_options.h"

namespace shirakami::testing {

using namespace shirakami;
using namespace std::chrono_literals;

class sequence_multiple_update_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-sequence-sequence_multiple_update_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
    }
    void SetUp() override {
        std::call_once(init_, call_once_f);
        init();
    }
    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(sequence_multiple_update_test, basic) { // NOLINT
    // update sequence
    SequenceId id{};
    {
        Token token{};
        ASSERT_EQ(enter(token), Status::OK);

        ASSERT_EQ(Status::OK, create_sequence(&id));
        SequenceVersion version{};
        ++version;
        SequenceValue value{1};
        auto ret =
                tx_begin({token, transaction_options::transaction_type::SHORT});
        if (ret != Status::OK) {
            LOG_FIRST_N(ERROR, 1)
                    << log_location_prefix << "unexpected error. " << ret;
        }
        ASSERT_EQ(Status::OK, update_sequence(token, id, version, value));
        ASSERT_EQ(Status::OK, commit(token)); // NOLINT
        std::this_thread::sleep_for(200ms);
        ++version;
        ret = tx_begin({token, transaction_options::transaction_type::SHORT});
        if (ret != Status::OK) {
            LOG_FIRST_N(ERROR, 1)
                    << log_location_prefix << "unexpected error. " << ret;
        }
        ASSERT_EQ(Status::OK, update_sequence(token, id, version, value));
        ASSERT_EQ(Status::OK, commit(token)); // NOLINT
        std::this_thread::sleep_for(200ms);
        ++version;
        ret = tx_begin({token, transaction_options::transaction_type::SHORT});
        if (ret != Status::OK) {
            LOG_FIRST_N(ERROR, 1)
                    << log_location_prefix << "unexpected error. " << ret;
        }
        ASSERT_EQ(Status::OK, update_sequence(token, id, version, value));
        ASSERT_EQ(Status::OK, commit(token)); // NOLINT
        std::this_thread::sleep_for(200ms);
        ++version;
        ret = tx_begin({token, transaction_options::transaction_type::SHORT});
        if (ret != Status::OK) {
            LOG_FIRST_N(ERROR, 1)
                    << log_location_prefix << "unexpected error. " << ret;
        }
        ASSERT_EQ(Status::OK, update_sequence(token, id, version, value));
        ASSERT_EQ(Status::OK, commit(token)); // NOLINT

        ASSERT_EQ(leave(token), Status::OK);
    }
    // read sequence
    {
#ifdef PWAL
        // wait updating result effect
        auto ce = epoch::get_global_epoch(); // current epoch
        for (;;) {
            if (epoch::get_datastore_durable_epoch() > ce) { break; }
            _mm_pause();
        }
#endif
        // verfiy
        SequenceVersion version{};
        SequenceValue value{};
        ASSERT_EQ(Status::OK, read_sequence(id, &version, &value));
        EXPECT_EQ(version, 4);
        EXPECT_EQ(value, 1);
    }
    // delete sequence
    {
        ASSERT_EQ(Status::OK, delete_sequence(id));
        ASSERT_EQ(Status::WARN_NOT_FOUND, delete_sequence(id));
    }
}

} // namespace shirakami::testing
