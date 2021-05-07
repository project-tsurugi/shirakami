/**
 * @file sequence_test.cpp
 */

#include "gtest/gtest.h"

#include "sequence.h"

#include "shirakami/interface.h"

namespace shirakami::testing {

using namespace shirakami;

class sequence_test : public ::testing::Test { // NOLINT
public:
    void SetUp() override { init(); } // NOLINT
    void TearDown() override { fin(); }
};

TEST_F(sequence_test, basic) { // NOLINT
#if defined(CPR)
    // todo
    ASSERT_EQ(true, true);
#else
    SequenceId id{};
    SequenceVersion ver{};
    SequenceValue val{};
    ASSERT_EQ(read_sequence(id, &ver, &val), Status::WARN_NOT_FOUND);
    ASSERT_EQ(create_sequence(&id), Status::OK);
    ASSERT_EQ(read_sequence(id, &ver, &val), Status::OK);
    ASSERT_EQ(ver, std::get<sequence_map::version_pos>(sequence_map::initial_value));
    ASSERT_EQ(val, std::get<sequence_map::value_pos>(sequence_map::initial_value));
    Token token{};
    ASSERT_EQ(update_sequence(token, id, ver + 1, val + 2), Status::OK);
    ASSERT_EQ(read_sequence(id, &ver, &val), Status::OK);
    ASSERT_EQ(ver, std::get<sequence_map::version_pos>(sequence_map::initial_value) + 1);
    ASSERT_EQ(val, std::get<sequence_map::value_pos>(sequence_map::initial_value) + 2);
    ASSERT_EQ(delete_sequence(token, id), Status::OK);
    ASSERT_EQ(read_sequence(id, &ver, &val), Status::WARN_NOT_FOUND);
#endif
}

} // namespace shirakami::testing