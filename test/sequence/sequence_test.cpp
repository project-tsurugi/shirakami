/**
 * @file sequence_test.cpp
 */

#include "gtest/gtest.h"

#include "sequence.h"

#if defined(CPR)

#include "fault_tolerance/include/cpr.h"

#endif

#include "shirakami/interface.h"

namespace shirakami::testing {

using namespace shirakami;

class sequence_test : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        init();
    }
    void TearDown() override { fin(); }
};

TEST_F(sequence_test, basic) { // NOLINT
    SequenceId id{};
    SequenceVersion ver{};
    SequenceValue val{};
    ASSERT_EQ(read_sequence(id, &ver, &val), Status::WARN_NOT_FOUND);
    ASSERT_EQ(create_sequence(&id), Status::OK);
#if defined(CPR)
    cpr::wait_next_checkpoint(); // wait to be able to read updated value.
#endif
    ASSERT_EQ(read_sequence(id, &ver, &val), Status::OK);
    ASSERT_EQ(ver, std::get<sequence_map::version_pos>(sequence_map::initial_value));
    ASSERT_EQ(val, std::get<sequence_map::value_pos>(sequence_map::initial_value));
    Token token{};
    while (enter(token) != Status::OK) {
        ;
    }
    tx_begin({token});
    // update_sequence depends on transaction semantics.
    ASSERT_EQ(update_sequence(token, id, ver + 1, val + 2), Status::OK);
    commit(token);
    leave(token);
#if defined(CPR)
    cpr::wait_next_checkpoint(); // wait to be able to read updated value.
#endif
    ASSERT_EQ(read_sequence(id, &ver, &val), Status::OK);
    ASSERT_EQ(ver, std::get<sequence_map::version_pos>(sequence_map::initial_value) + 1);
    ASSERT_EQ(val, std::get<sequence_map::value_pos>(sequence_map::initial_value) + 2);
    ASSERT_EQ(delete_sequence(id), Status::OK);
#if defined(CPR)
    cpr::wait_next_checkpoint(); // wait to be able to read updated value.
#endif
    ASSERT_EQ(read_sequence(id, &ver, &val), Status::WARN_NOT_FOUND);
}

} // namespace shirakami::testing