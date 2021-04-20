
#include "gtest/gtest.h"


#ifdef RECOVERY

#include "boost/filesystem.hpp"

#endif

#include "shirakami/interface.h"
namespace shirakami::testing {

using namespace shirakami;

Storage storage;
class phantom_protection : public ::testing::Test {
public:
    void SetUp() override {
#if defined(RECOVERY)
        std::string path{MAC2STR(PROJECT_ROOT)}; // NOLINT
        path += "/log/checkpoint";
        if (boost::filesystem::exists(path)) {
            boost::filesystem::remove(path);
        }
#endif
        init(); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(phantom_protection, phantom) { // NOLINT
    register_storage(storage);
    constexpr std::size_t token_length{2};
    std::array<Token, token_length> token{};
    ASSERT_EQ(enter(token.at(0)), Status::OK);
    ASSERT_EQ(enter(token.at(1)), Status::OK);
    constexpr std::size_t key_length{3};
    std::array<std::string, 3> key;
    for (std::size_t i = 0; i < key_length; ++i) {
        key.at(i) = std::string{1, static_cast<char>(i)}; // NOLINT
    }
    std::string v{"value"}; // NOLINT
    ASSERT_EQ(Status::OK, insert(token.at(0), storage, key.at(0), v));
    ASSERT_EQ(Status::OK, insert(token.at(0), storage, key.at(1), v));
    ASSERT_EQ(Status::OK, commit(token.at(0))); // NOLINT
    std::vector<const Tuple*> tuple_vec;
    ASSERT_EQ(Status::OK,
              scan_key(token.at(0), storage, "", scan_endpoint::INF, "", scan_endpoint::INF, tuple_vec));
    ASSERT_EQ(tuple_vec.size(), 2);
    // interrupt to occur phantom
    ASSERT_EQ(Status::OK, insert(token.at(1), storage, key.at(2), v));
    ASSERT_EQ(Status::OK, commit(token.at(1))); // NOLINT
    // =====
    ASSERT_EQ(Status::ERR_PHANTOM, commit(token.at(0))); // NOLINT
    // abort by phantom
    ASSERT_EQ(leave(token.at(0)), Status::OK);
    ASSERT_EQ(leave(token.at(1)), Status::OK);
}

} // namespace shirakami::testing
