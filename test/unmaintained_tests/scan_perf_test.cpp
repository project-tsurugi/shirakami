#include "shirakami/interface.h"
#include "gtest/gtest.h"

// shirakami-impl interface library
#include "tsc.h"

namespace shirakami::testing {

using namespace shirakami;

constexpr const int MAX_TUPLES = 1000000;
constexpr const int READ_TUPLES = 100;
Storage storage;

class scan_perf : public ::testing::Test {
public:
    void DoInsert(int, int);

    void DoScan();

    Token& get_s() { return s_; } // NOLINT

    void SetUp() override {
        init(); // NOLINT
        for (int i = 0; i < MAX_TUPLES; ++i) {
            key_.at(i) = i;
        }
    }

    void TearDown() override { fin(); }

private:
    std::array<std::uint64_t, MAX_TUPLES> key_{};
    Token s_{};
};

void scan_perf::DoInsert(int bgn_idx, int end_idx) {
    std::string v1("a"); // NOLINT

    for (int i = bgn_idx; i < end_idx; ++i) {
        EXPECT_EQ(Status::OK, insert(s_, storage, {reinterpret_cast<char*>(&key_.at(i)), sizeof(key_.at(i))}, v1)); // NOLINT
    }
    EXPECT_EQ(Status::OK, commit(s_));
}

void scan_perf::DoScan() {
    std::uint64_t scan_size{};
    ScanHandle handle{};
    Tuple* tuple{};

    std::uint64_t start{rdtscp()};
    EXPECT_EQ(Status::OK, open_scan(s_, storage, "", scan_endpoint::INF, "", scan_endpoint::INF, handle));
    for (int i = 0; i < READ_TUPLES; ++i) {
        EXPECT_EQ(Status::OK, read_from_scan(s_, handle, &tuple));
    }
    /**
     * Make sure the scan size.
     */
    EXPECT_EQ(Status::OK, scannable_total_index_size(s_, handle, scan_size));
    std::cout << "scannable_total_index_size : " << scan_size << std::endl;
    EXPECT_EQ(Status::OK, commit(s_));
    std::uint64_t end{rdtscp()};
    std::cout << "Result : " << end - start << " [clocks]" << std::endl;
}

TEST_F(scan_perf, read_from_scan) { // NOLINT
    register_storage(storage);
    EXPECT_EQ(Status::OK, enter(get_s()));

    DoInsert(0, 100); // NOLINT
    std::cout << "Perform 100 records read_from_scan on a table with 100 records."
              << std::endl;
    DoScan();

    /**
     * Prepare for next experiments.
     */
    DoInsert(100, 1000); // NOLINT
    std::cout << "Perform 100 records read_from_scan on a table with 1K records."
              << std::endl;
    DoScan();

    /**
     * Prepare for next experiments.
     */
    DoInsert(1000, 10000); // NOLINT
    std::cout << "Perform 100 records read_from_scan on a table with 10K records."
              << std::endl;
    DoScan();

    /**
     * Prepare for next experiments.
     */
    DoInsert(10000, 20000); // NOLINT
    std::cout << "Perform 100 records read_from_scan on a table with 20K records."
              << std::endl;
    DoScan();

    /**
     * Prepare for next experiments.
     */
    DoInsert(20000, 40000); // NOLINT
    std::cout << "Perform 100 records read_from_scan on a table with 40K records."
              << std::endl;
    DoScan();

    /**
     * Prepare for next experiments.
     */
    DoInsert(40000, 80000); // NOLINT
    std::cout << "Perform 100 records read_from_scan on a table with 80K records."
              << std::endl;
    DoScan();

    /**
     * Prepare for next experiments.
     */
    DoInsert(80000, 160000); // NOLINT
    std::cout
            << "Perform 100 records read_from_scan on a table with 160K records."
            << std::endl;
    DoScan();

    /**
     * Prepare for next experiments.
     */
    DoInsert(160000, 320000); // NOLINT
    std::cout
            << "Perform 100 records read_from_scan on a table with 320K records."
            << std::endl;
    DoScan();

    EXPECT_EQ(Status::OK, leave(get_s()));
}

} // namespace shirakami::testing
