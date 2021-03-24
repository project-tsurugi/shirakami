
#include "gtest/gtest.h"

#include <tsl/hopscotch_map.h>

namespace shirakami::testing {

class hopscotch_map_test : public ::testing::Test { // NOLINT
};

TEST_F(hopscotch_map_test, basic_operation) { // NOLINT
    tsl::hopscotch_map<std::string, std::string> map;
    ASSERT_EQ(map.size(), 0);
    // prepare data
    std::array<std::pair<std::string, std::string>, 3> kv{std::make_pair("k1", "v1"), std::make_pair("k2", "v2"), std::make_pair("k3", "v3")};
    // initialize map
    for (auto&& elem : kv) {
        map[elem.first] = elem.second;
    }
    // check map size
    ASSERT_EQ(map.size(), kv.size());
    // check finding and update
    map.find(kv.at(0).first).value() = "v1-2";
    // check effect of last work
    ASSERT_EQ(map.find(kv.at(0).first).value(), "v1-2");
    // check non-exisiting key
    ASSERT_EQ(map.find("kkk"), map.end());
    // check existing key
    for (auto&& elem : kv) {
        ASSERT_NE(map.find(elem.first), map.end());
    }
    // check removing existing key
    for (auto&& elem : kv) {
        ASSERT_EQ(map.erase(elem.first), 1);
    }
    // check removing non-exisiting key
    ASSERT_EQ(map.erase(kv.at(0).first), 0);
}

} // namespace shirakami::testing
