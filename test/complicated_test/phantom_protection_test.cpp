
#include "gtest/gtest.h"


#ifdef RECOVERY

#include "boost/filesystem.hpp"

#endif

#include "shirakami/interface.h"

#include "concurrency_control/include/session_info.h"

namespace shirakami::testing {

using namespace shirakami;

Storage storage;
class phantom_protection : public ::testing::Test {
public:
    void SetUp() override {
        std::string log_dir{MAC2STR(PROJECT_ROOT)}; // NOLINT
        log_dir.append("/build/phantom_protection_test_log");
        init(false, log_dir); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(phantom_protection, phantom_basic) { // NOLINT
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
#if defined(CPR)
    while (Status::OK != scan_key(token.at(0), storage, "", scan_endpoint::INF, "", scan_endpoint::INF, tuple_vec)) {
        ;
    }
#else
    ASSERT_EQ(Status::OK, scan_key(token.at(0), storage, "", scan_endpoint::INF, "", scan_endpoint::INF, tuple_vec));
#endif
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

TEST_F(phantom_protection, phantom_no_elem_nodes) { // NOLINT
    /**
     * conditions:
     * structure: tree which has some fanout.
     * A: 1,
     * B: 51,
     * C: 102
     * branch of A and B is 3,
     * branch of B and C is 90.
     * range [2:100].
     * Under the above conditions, the result of the scan should be "51",
     * but if it is interrupted by the insertion of "2" or "101",
     * phantom avoidance must be performed on the nodes A and C.
     */
    register_storage(storage);
    Token token{};
    std::string v{"v"};
    ASSERT_EQ(enter(token), Status::OK);
    for (char i = 0; i <= 25; ++i) { // NOLINT
        char c = i;
        ASSERT_EQ(insert(token, storage, std::string_view(&c, 1), v), Status::OK);
    }
    ASSERT_EQ(Status::OK, commit(token));
    /**
     * now,
     * A:  0,  1,  2,  3,  4,  5,  6,  7, 8
     * B:  9, 10, 11, 12, 13, 14, 15, 16, 17
     * C: 18, 19, 20, 21, 22, 23, 24, 25
     * branch of A and B is 9
     * branch of B and C is 18
     */

    auto delete_range = [&token](char begin, char end) {
        for (char i = begin; i <= end; ++i) {
            char c = i;
            ASSERT_EQ(delete_record(token, storage, std::string_view(&c, 1)), Status::OK);
        }
        ASSERT_EQ(Status::OK, commit(token));
    };
    delete_range(1, 8); // NOLINT
    delete_range(18, 24); // NOLINT
    delete_range(17, 17); // NOLINT
    /**
     * now,
     * A:  0, 
     * B:  9, 10, 11, 12, 13, 14, 15, 16
     * C:  25
     * branch of A and B is 9
     * branch of B and C is 18
     */

    Token enemy{};
    ASSERT_EQ(enter(enemy), Status::OK);
    // easy phantom
    char begin{9}; // NOLINT
    char end{17}; // NOLINT
    std::vector<const Tuple*> res;
    /**
     * for unhook latency.
     * silo's physical unhook is postponed because it needs to link order of epoch of past deleted record and inserted record in the future.
     */
    sleep(1);
    ASSERT_EQ(scan_key(token, storage, std::string_view(&begin, 1), scan_endpoint::INCLUSIVE, std::string_view(&end, 1), scan_endpoint::INCLUSIVE, res), Status::OK);
    ASSERT_EQ(res.size(), 8);
    char ekey{17}; // NOLINT
    ASSERT_EQ(insert(enemy, storage, std::string_view(&ekey, 1), v), Status::OK);
    ASSERT_EQ(commit(enemy), Status::OK);
    /**
     * now,
     * A:  0, 
     * B:  9, 10, 11, 12, 13, 14, 15, 16, 17
     * C:  25
     * branch of A and B is 9
     * branch of B and C is 18
     */
    ASSERT_EQ(commit(token), Status::ERR_PHANTOM);

    begin = 0; // NOLINT
    end = 0; // NOLINT
    ASSERT_EQ(scan_key(token, storage, std::string_view(&begin, 1), scan_endpoint::INCLUSIVE, std::string_view(&end, 1), scan_endpoint::INCLUSIVE, res), Status::OK);
    auto* ti = static_cast<session_info*>(token);
    yakushima::node_version64* node_a{std::get<1>(ti->get_node_set().back())};
    ASSERT_EQ(commit(token), Status::OK);
    begin = 9; // NOLINT
    end = 9; // NOLINT
    ASSERT_EQ(scan_key(token, storage, std::string_view(&begin, 1), scan_endpoint::INCLUSIVE, std::string_view(&end, 1), scan_endpoint::INCLUSIVE, res), Status::OK);
    yakushima::node_version64* node_b{std::get<1>(ti->get_node_set().back())};
    ASSERT_EQ(commit(token), Status::OK);
    begin = 25; // NOLINT
    end = 25; // NOLINT
    ASSERT_EQ(scan_key(token, storage, std::string_view(&begin, 1), scan_endpoint::INCLUSIVE, std::string_view(&end, 1), scan_endpoint::INCLUSIVE, res), Status::OK);
    yakushima::node_version64* node_c{std::get<1>(ti->get_node_set().back())};
    ASSERT_EQ(commit(token), Status::OK);
    /**
     * check whether elements 0 / 9 / 25 are at independent states from each other.
     */
    ASSERT_NE(node_a, node_b);
    ASSERT_NE(node_b, node_c);

/**
     * We want to pass this block later.
     */
#if 0
    begin = 7;
    end = 20;
    ASSERT_EQ(scan_key(token, storage, std::string_view(&begin, 1), scan_endpoint::INCLUSIVE, std::string_view(&end, 1), scan_endpoint::INCLUSIVE, res), Status::OK);
    ASSERT_EQ(res.size(), 9);
    // todo: it should path.
    // ti = static_cast<session_info*>(token);
    // ASSERT_EQ(ti->get_node_set().size(), 3); 
    ekey = 7;
    ASSERT_EQ(insert(enemy, storage, std::string_view(&ekey, 1), v), Status::OK);
    ASSERT_EQ(commit(enemy), Status::OK);
    /**
     * now,
     * A:  0, 7
     * B:  9, 10, 11, 12, 13, 14, 15, 16, 17
     * C:  25
     * branch of A and B is 9
     * branch of B and C is 18
     */
    ASSERT_EQ(commit(token), Status::ERR_PHANTOM);

    begin = 8;
    end = 20;
    ASSERT_EQ(scan_key(token, storage, std::string_view(&begin, 1), scan_endpoint::INCLUSIVE, std::string_view(&end, 1), scan_endpoint::INCLUSIVE, res), Status::OK);
    ASSERT_EQ(res.size(), 9);
    ekey = 19;
    ASSERT_EQ(insert(enemy, storage, std::string_view(&ekey, 1), v), Status::OK);
    ASSERT_EQ(commit(enemy), Status::OK);
    /**
     * now,
     * A:  0, 7
     * B:  9, 10, 11, 12, 13, 14, 15, 16, 17
     * C:  19, 25
     * branch of A and B is 9
     * branch of B and C is 18
     */
    ASSERT_EQ(commit(token), Status::ERR_PHANTOM);
#endif

    ASSERT_EQ(leave(token), Status::OK);
    ASSERT_EQ(leave(enemy), Status::OK);
}

} // namespace shirakami::testing
