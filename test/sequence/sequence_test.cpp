/**
 * @file sequence_test.cpp
 */

#include <emmintrin.h>
#include <memory>
#include <mutex>

#include "shirakami/interface.h"
#include "gtest/gtest.h"
#include "glog/logging.h"
#include "concurrency_control/include/epoch.h"
#include "shirakami/api_sequence.h"
#include "shirakami/binary_printer.h"
#include "shirakami/scheme.h"
#include "shirakami/transaction_options.h"

namespace shirakami::testing {

using namespace shirakami;

class sequence_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-sequence-sequence_test");
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

TEST_F(sequence_test, basic) { // NOLINT
    // create sequence
    {
        SequenceId id{};
        ASSERT_EQ(Status::OK, create_sequence(&id));
    }
    // update sequence
    SequenceId id{};
    {
        Token token{};
        ASSERT_EQ(enter(token), Status::OK);
        SequenceValue value{};
        ASSERT_EQ(Status::OK, create_sequence(&id));
        SequenceVersion version{};
        ASSERT_EQ(Status::OK,
                  tx_begin({token,
                            transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, update_sequence(token, id, version, value));
        ASSERT_EQ(Status::WARN_ALREADY_EXISTS,
                  update_sequence(token, id, version, value));
        /**
          * because it executed same operation (especially same
          * version) by same arguments.
          */
        ASSERT_EQ(Status::OK, commit(token));
        ASSERT_EQ(Status::OK,
                  tx_begin({token,
                            transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, update_sequence(token, id, version, value));
        ASSERT_EQ(Status::OK, commit(token));
        // this update_sequence is invalid because version is initial (0);
        version = 1;
        ASSERT_EQ(Status::OK,
                  tx_begin({token,
                            transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, update_sequence(token, id, version, value));
        ASSERT_EQ(Status::OK, commit(token));
        // this update_sequence is valid.
        version = 2;
        value = 3;
        ASSERT_EQ(Status::OK,
                  tx_begin({token,
                            transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, update_sequence(token, id, version, value));
        ASSERT_EQ(Status::OK, commit(token));
        ASSERT_EQ(Status::OK,
                  tx_begin({token,
                            transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, update_sequence(token, id, version, value));
        ASSERT_EQ(Status::OK, commit(token));
        // this update_sequence is invalid because version is 2 yet.
        ASSERT_EQ(Status::OK,
                  tx_begin({token,
                            transaction_options::transaction_type::SHORT}));
        ASSERT_EQ(Status::OK, update_sequence(token, id, version, value));
        ASSERT_EQ(Status::WARN_ALREADY_EXISTS,
                  update_sequence(token, id, version - 1, value));
        /**
          * because it executed same operation by larger version.
          */
        ASSERT_EQ(Status::OK, commit(token));
        // this update_sequence is invalid because version is 2 yet.
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
        ASSERT_EQ(version, 2);
        ASSERT_EQ(value, 3);
    }
    // delete sequence
    {
        ASSERT_EQ(Status::OK, delete_sequence(id));
        ASSERT_EQ(Status::WARN_NOT_FOUND, delete_sequence(id));
    }
}

} // namespace shirakami::testing
