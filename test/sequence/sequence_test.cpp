/**
 * @file sequence_test.cpp
 */

#include "gtest/gtest.h"

#include "sequence.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class sequence_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-sequence-sequence_test");
        FLAGS_stderrthreshold = 0; // output more than INFO
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
    {
        Token token{};
        SequenceId id{};
        SequenceVersion version{};
        SequenceValue value{};
        ASSERT_EQ(Status::OK,
                  update_sequence(token, id, version, value));
    }
    // read sequence
    {
        SequenceId id{};
        SequenceVersion version{};
        SequenceValue value{};
        ASSERT_EQ(Status::OK,
                  read_sequence(id, &version, &value));
    }
    // delete sequence
    {
        SequenceId id{};
        ASSERT_EQ(Status::OK, delete_sequence(id));
    }
}

} // namespace shirakami::testing