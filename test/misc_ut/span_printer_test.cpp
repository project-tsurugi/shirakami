
#include <sstream>

#include "glog/logging.h"
#include "gtest/gtest.h"

#include "shirakami/span_printer.h"

namespace shirakami::testing {

using namespace shirakami;

class span_printer_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-misc_ut-span_printer_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google;
};

template<typename T>
std::string print(const std::vector<T>& v) {
    std::stringstream ss{};
    ss << span_printer(v);
    return ss.str();
}

template<typename T>
std::string print(const std::vector<T>& v, std::string_view s) {
    std::stringstream ss{};
    ss << span_printer(v, s);
    return ss.str();
}

template<typename T>
std::string print(const T* p, std::size_t n) {
    std::stringstream ss{};
    ss << span_printer(p, n);
    return ss.str();
}

template<typename T>
std::string print(const T* p, std::size_t n, std::string_view s) {
    std::stringstream ss{};
    ss << span_printer(p, n, s);
    return ss.str();
}

TEST_F(span_printer_test, charvec) {
    EXPECT_EQ(print(std::vector<char>{}), "[]");
    EXPECT_EQ(print(std::vector<char>{'a'}), "[a]");
    EXPECT_EQ(print(std::vector<char>{'a', 'b'}), "[a, b]");
    EXPECT_EQ(print(std::vector<char>{'c', 'h', 'a', 'r'}, ""), "[char]");
}

TEST_F(span_printer_test, uint64vec) {
    EXPECT_EQ(print(std::vector<std::uint64_t>{}), "[]");
    EXPECT_EQ(print(std::vector<std::uint64_t>{0}), "[0]");
    EXPECT_EQ(print(std::vector<std::uint64_t>{100, 200}), "[100, 200]");
    EXPECT_EQ(print(std::vector<std::uint64_t>{}, "x"), "[]");
    EXPECT_EQ(print(std::vector<std::uint64_t>{100, 200, 30}, "x"), "[100x200x30]");
}

TEST_F(span_printer_test, uint64arr) {
    EXPECT_EQ(print((std::uint64_t*)nullptr, 0), "[]");
    std::uint64_t p1[] = {0U, 1U, 2U, 3U, 4U, 5U};
    EXPECT_EQ(print(p1, 1), "[0]");
    EXPECT_EQ(print(p1, 2), "[0, 1]");
    EXPECT_EQ(print(&p1[3], 3, "x"), "[3x4x5]");
}

} // namespace shirakami::testing
