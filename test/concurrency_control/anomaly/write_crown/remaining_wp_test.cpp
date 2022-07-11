
#include <xmmintrin.h>

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "atomic_wrapper.h"

#include "concurrency_control/wp/include/epoch.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/version.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class remaining_wp_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-wp-remaining_wp_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

void generate_test_case(
        // bool shows it can commit.
        std::vector<std::tuple<std::array<transaction_options::transaction_type, 4>, std::array<bool, 4>>>&
                test_case) {
    auto add_test = [&test_case](transaction_options::transaction_type ib1, transaction_options::transaction_type ib2, transaction_options::transaction_type ib3,
                                 transaction_options::transaction_type ib4, bool cc1, bool cc2, bool cc3,
                                 bool cc4) {
        using t_type = std::array<transaction_options::transaction_type, 4>;
        using c_type = std::array<bool, 4>;
        test_case.emplace_back(std::make_tuple(t_type{ib1, ib2, ib3, ib4},
                                               c_type{cc1, cc2, cc3, cc4}));
    };

    add_test(transaction_options::transaction_type::SHORT, transaction_options::transaction_type::SHORT, transaction_options::transaction_type::SHORT, // NOLINT
             transaction_options::transaction_type::SHORT, 0, 1, 1, 1);                    // NOLINT
    // Batch mode can only be tested after w-w constraint relaxation.
}

void gen_initial_db(Storage st) {
    std::string a{"a"};
    std::string x{"x"};
    std::string y{"y"};
    std::string z{"z"};
    Token s0{};
    ASSERT_EQ(enter(s0), Status::OK);
    ASSERT_EQ(upsert(s0, st, x, ""), Status::OK);
    ASSERT_EQ(upsert(s0, st, y, ""), Status::OK);
    ASSERT_EQ(upsert(s0, st, z, ""), Status::OK);
    ASSERT_EQ(upsert(s0, st, a, ""), Status::OK);
    ASSERT_EQ(commit(s0), Status::OK);
    ASSERT_EQ(leave(s0), Status::OK);
}

TEST_F(remaining_wp_test, simple) { // NOLINT
                                    // create table
    Storage st{};
    ASSERT_EQ(create_storage(st), Status::OK);

    // enter
    std::array<Token, 4> s{};
    for (auto&& elem : s) { ASSERT_EQ(enter(elem), Status::OK); }

    // prepare page key value
    std::string a{"a"};
    std::string x{"x"};
    std::string y{"y"};
    std::string z{"z"};
    std::array<std::string, 4> v{"t1", "t2", "t3", "t4"};

    // prepare test case container
    // tuple.first : std::array: <is_batch, 4>
    // tuple.second: std::array: <can_commit, 4>
    std::vector<std::tuple<std::array<transaction_options::transaction_type, 4>, std::array<bool, 4>>>
            test_case;

    // generate test case
    generate_test_case(test_case);

    for (auto&& tc : test_case) {
        init(); // NOLINT
        gen_initial_db(st);
        std::string vb{};
        ASSERT_EQ(tx_begin({s.at(0), std::get<0>(tc).at(0)}), Status::OK);
        ASSERT_EQ(search_key(s.at(0), st, x, vb), Status::OK);
        ASSERT_EQ(tx_begin({s.at(1), std::get<0>(tc).at(1)}), Status::OK);
        ASSERT_EQ(search_key(s.at(1), st, y, vb), Status::OK);
        ASSERT_EQ(tx_begin({s.at(2), std::get<0>(tc).at(2)}), Status::OK);
        ASSERT_EQ(search_key(s.at(2), st, z, vb), Status::OK);
        ASSERT_EQ(tx_begin({s.at(3), std::get<0>(tc).at(3)}), Status::OK);
        ASSERT_EQ(search_key(s.at(3), st, a, vb), Status::OK);
        ASSERT_EQ(upsert(s.at(3), st, x, v.at(3)), Status::OK);
        ASSERT_EQ(commit(s.at(3)), Status::OK);
        ASSERT_EQ(upsert(s.at(2), st, a, v.at(2)), Status::OK);
        ASSERT_EQ(commit(s.at(2)), Status::OK);
        ASSERT_EQ(upsert(s.at(1), st, z, v.at(1)), Status::OK);
        ASSERT_EQ(commit(s.at(1)), Status::OK);
        ASSERT_EQ(upsert(s.at(0), st, y, v.at(0)), Status::OK);
        ASSERT_EQ(commit(s.at(0)), Status::ERR_VALIDATION);
        fin();
        LOG(INFO) << "clear test case " << std::get<0>(tc).at(0) << ", "
                  << std::get<0>(tc).at(1) << ", " << std::get<0>(tc).at(2)
                  << ", " << std::get<0>(tc).at(3) << ", "
                  << std::get<1>(tc).at(0) << ", " << std::get<1>(tc).at(1)
                  << ", " << std::get<1>(tc).at(2) << ", "
                  << std::get<1>(tc).at(3);
    }

    // leave
    for (auto&& elem : s) { ASSERT_EQ(leave(elem), Status::OK); }
}

} // namespace shirakami::testing
