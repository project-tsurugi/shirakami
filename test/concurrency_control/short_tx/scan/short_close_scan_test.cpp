
#include <string>

#include "shirakami/interface.h"
#include "gtest/gtest.h"
#include "shirakami/api_diagnostic.h"
#include "shirakami/api_storage.h"
#include "shirakami/scheme.h"
#include "shirakami/storage_options.h"
#include "shirakami/transaction_options.h"

namespace shirakami::testing {

using namespace shirakami;

class simple_scan : public ::testing::Test { // NOLINT
public:
    void SetUp() override {
        init(); // NOLINT
    }

    void TearDown() override { fin(); }
};

TEST_F(simple_scan, close_scan_with_not_begin) { // NOLINT
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::WARN_NOT_BEGIN, close_scan(s, {}));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_scan, close_scan) { // NOLINT
    Storage storage{};
    create_storage("", storage);
    std::string k1("a"); // NOLINT
    std::string v1("0"); // NOLINT
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, storage, k1, v1));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ScanHandle handle{};
    ASSERT_EQ(Status::OK, open_scan(s, storage, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, handle));
    ASSERT_EQ(Status::OK, close_scan(s, handle));
    ASSERT_EQ(Status::WARN_INVALID_HANDLE, close_scan(s, handle));
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
