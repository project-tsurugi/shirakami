
#include <glog/logging.h>

#include <mutex>

#include "clock.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class comp1_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-complicated-comp1_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(comp1_test, test1) { // NOLINT
    /**
     * scenario.
     * 1: scan read 14 entry
     * 2: create sequence
     * 3: read sequence id 15
     * 4: upsert 14 entry + new 1 entry
     * 5: get_storage "test" twice, WARN_NOT_FOUND
     * 6: create_storage "test" and get_storage "test"
     */
    // prepare
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // prepare
    for (auto i = 0; i < 14; ++i) { // NOLINT
        std::string key(1, i);
        std::string value(std::to_string(i));
        ASSERT_EQ(upsert(s, st, key, value), Status::OK);
    }
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ScanHandle handle{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, handle));
    // read all range
    for (auto i = 0; i < 14; ++i) { // NOLINT
        std::string sb{};
        ASSERT_EQ(Status::OK, read_key_from_scan(s, handle, sb));
        ASSERT_EQ(sb, std::string(1, i));
        ASSERT_EQ(Status::OK, read_value_from_scan(s, handle, sb));
        ASSERT_EQ(sb, std::to_string(i));
        if (i != 13) { // NOLINT
            ASSERT_EQ(next(s, handle), Status::OK);
        } else {
            ASSERT_EQ(next(s, handle), Status::WARN_SCAN_LIMIT);
        }
    }
    ASSERT_EQ(close_scan(s, handle), Status::OK);

    // 2
    SequenceId sid{};
    ASSERT_EQ(Status::OK, create_sequence(&sid));

    // 3
    SequenceVersion sver{};
    SequenceValue sval{};
    ASSERT_EQ(Status::WARN_NOT_FOUND, read_sequence(15, &sver, &sval));

    // 4
    for (auto i = 0; i < 14; ++i) { // NOLINT
        std::string key(1, i);
        std::string value(std::to_string(i));
        ASSERT_EQ(upsert(s, st, key, value), Status::OK);
    }
    std::string key(1, 14);                // NOLINT
    std::string value(std::to_string(14)); // NOLINT
    ASSERT_EQ(upsert(s, st, key, value), Status::OK);

    // 5
    Storage st_out{};
    ASSERT_EQ(get_storage("test", st_out), Status::WARN_NOT_FOUND);
    ASSERT_EQ(get_storage("test", st_out), Status::WARN_NOT_FOUND);

    // 6
    ASSERT_EQ(create_storage("test", st_out), Status::OK);
    ASSERT_EQ(get_storage("test", st_out), Status::OK);

    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing