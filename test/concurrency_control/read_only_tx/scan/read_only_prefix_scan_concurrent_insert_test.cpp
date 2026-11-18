// Reproduction of wrong results of the range scan by read-only transaction in iterator-based scan mode.
//
// Every round:
// - insert 15 keys of 9 bytes, 0x01 + big-endian(v) (v = 0..14), into a new storage
// - begin a read-only tx which sees the 15 keys
// - for each key k, range scan [k INCLUSIVE, k + 1 EXCLUSIVE) repeatedly, which should return exactly k, while
//   another thread inserts the 16th key (v = 15) by short tx after a random delay (0 to 1 ms)
//
// The range scan returns entries beyond the end point without errors in about half of the rounds. It did not happen
// - in legacy scan mode (set_scan_mode_iterator_based(false))
// - with 14 keys initially and inserting the 15th
// - without the concurrent insert
// - with 8-byte keys big-endian(v), or 9-byte keys big-endian(v) + 0x01

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "concurrency_control/include/scan.h"

#include "shirakami/interface.h"

#include "gtest/gtest.h"

#include "glog/logging.h"

namespace shirakami::testing {

using namespace shirakami;

class read_only_prefix_scan_concurrent_insert_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-read_only_prefix_scan_concurrent_insert_test");
    }

    void SetUp() override {
        std::call_once(init_google, call_once_f);
        init(); // NOLINT
        set_scan_mode_iterator_based(true);
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google;
};

namespace {

std::string key_of(std::uint64_t v) {
    std::string s{"\x01"};
    for (int i = 7; i >= 0; --i) { s.push_back(static_cast<char>((v >> (static_cast<unsigned>(i) * 8U)) & 0xffU)); }
    return s;
}

// the smallest key larger than all keys prefixed by the given key
std::string next_neighbor(std::string key) {
    while (! key.empty()) {
        auto& c = key.back();
        if (static_cast<unsigned char>(c) != 0xffU) {
            c = static_cast<char>(static_cast<unsigned char>(c) + 1U);
            return key;
        }
        key.pop_back();
    }
    return key;
}

std::string hex(std::string_view s) {
    std::ostringstream ss{};
    for (std::size_t i = 0; i < s.size(); ++i) {
        if (i != 0) { ss << '-'; }
        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<unsigned>(static_cast<unsigned char>(s[i]));
    }
    return ss.str();
}

std::vector<std::string> scan_keys(Token t, Storage st, std::string_view begin, scan_endpoint begin_ep,
                                   std::string_view end, scan_endpoint end_ep) {
    std::vector<std::string> ret{};
    ScanHandle hd{};
    Status rc{};
    // wait for the read-only tx to become ready
    while ((rc = open_scan(t, st, begin, begin_ep, end, end_ep, hd)) == Status::WARN_PREMATURE) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (rc == Status::WARN_NOT_FOUND) { return ret; }
    EXPECT_EQ(Status::OK, rc);
    if (rc != Status::OK) { return ret; }
    do {
        std::string k{};
        if (read_key_from_scan(t, hd, k) == Status::OK) { ret.emplace_back(k); }
    } while (next(t, hd) == Status::OK);
    EXPECT_EQ(Status::OK, close_scan(t, hd));
    return ret;
}

} // namespace

TEST_F(read_only_prefix_scan_concurrent_insert_test, split_while_scanning) { // NOLINT
    constexpr std::size_t rounds = 100;
    constexpr std::size_t initial = 15;       // number of keys the read-only tx sees (capacity of the border node)
    constexpr std::size_t max_delay_us = 1000; // max delay of the concurrent insert

    std::atomic<Storage> target{};
    std::atomic_size_t go{0};    // round for which the writer inserts
    std::atomic_size_t done{0};  // round for which the writer finished inserting
    std::atomic_bool stop{false};
    std::thread writer{[&]() {
        Token t{};
        ASSERT_EQ(Status::OK, enter(t));
        std::size_t round = 0;
        std::uint64_t rnd = 12345;
        while (! stop) {
            if (go.load() == round) {
                std::this_thread::yield();
                continue;
            }
            round = go.load();
            rnd = rnd * 6364136223846793005ULL + 1442695040888963407ULL;
            std::this_thread::sleep_for(std::chrono::microseconds((rnd >> 33U) % (max_delay_us + 1)));
            ASSERT_EQ(Status::OK, tx_begin({t, transaction_options::transaction_type::SHORT}));
            ASSERT_EQ(Status::OK, insert(t, target.load(), key_of(initial), ""));
            ASSERT_EQ(Status::OK, commit(t)); // NOLINT
            done = round;
        }
        ASSERT_EQ(Status::OK, leave(t));
    }};

    Token r{};
    ASSERT_EQ(Status::OK, enter(r));
    std::size_t checked = 0;
    std::size_t anomalies = 0;
    std::size_t bad_rounds = 0;
    for (std::size_t round = 1; round <= rounds; ++round) {
        Storage st{};
        ASSERT_EQ(Status::OK, create_storage("s" + std::to_string(round), st));
        for (std::uint64_t v = 0; v < initial; ++v) {
            ASSERT_EQ(Status::OK, tx_begin({r, transaction_options::transaction_type::SHORT}));
            ASSERT_EQ(Status::OK, insert(r, st, key_of(v), ""));
            ASSERT_EQ(Status::OK, commit(r)); // NOLINT
        }
        ASSERT_EQ(Status::OK, tx_begin({r, transaction_options::transaction_type::READ_ONLY}));
        std::vector<std::string> keys{};
        while ((keys = scan_keys(r, st, "", scan_endpoint::INF, "", scan_endpoint::INF)).size() < initial) {
            // the snapshot may not include all the keys yet
            ASSERT_EQ(Status::OK, commit(r)); // NOLINT
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            ASSERT_EQ(Status::OK, tx_begin({r, transaction_options::transaction_type::READ_ONLY}));
        }
        // let the writer insert the 16th key, and keep scanning until it's done (and one more pass after that)
        target = st;
        go = round;
        std::size_t bad_in_round = 0;
        while (true) {
            bool last_pass = done.load() == round;
            for (auto&& k : keys) {
                auto end = next_neighbor(k);
                auto found = scan_keys(r, st, k, scan_endpoint::INCLUSIVE, end, scan_endpoint::EXCLUSIVE);
                ++checked;
                if (found.size() != 1 || found.front() != k) {
                    ++anomalies;
                    ++bad_in_round;
                    std::cerr << "round:" << round << " begin:" << hex(k) << " end:" << hex(end)
                              << " found:" << found.size() << std::endl;
                    for (auto&& f : found) { std::cerr << "    " << hex(f) << std::endl; }
                }
            }
            if (last_pass) { break; }
        }
        ASSERT_EQ(Status::OK, commit(r)); // NOLINT
        if (bad_in_round != 0) { ++bad_rounds; }
    }
    ASSERT_EQ(Status::OK, leave(r));
    stop = true;
    writer.join();
    std::cerr << "rounds:" << rounds << " bad_rounds:" << bad_rounds << " checked:" << checked
              << " anomalies:" << anomalies << std::endl;
    EXPECT_EQ(0, anomalies);
}

} // namespace shirakami::testing
