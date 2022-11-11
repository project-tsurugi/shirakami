
#include <glog/logging.h>

#include <mutex>

#include "clock.h"

#include "test_tool.h"

#include "concurrency_control/include/epoch.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace shirakami;

namespace shirakami::testing {

class comp_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "shirakami-test-concurrency_control-complicated-comp_test");
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

TEST_F(comp_test, test1) { // NOLINT
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

TEST_F(comp_test, DISABLED_test2) { // NOLINT
    Storage st{};
    ASSERT_EQ(Status::OK, create_storage("", st));
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();

    std::string a{"a"};
    std::string b{"b"};
    std::string c{"c"};
    ASSERT_EQ(Status::OK, insert(s, st, a, a));
    ASSERT_EQ(Status::OK, insert(s, st, b, b));
    ASSERT_EQ(Status::OK, insert(s, st, c, c));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::LONG, {st}}));
    wait_epoch_update();
    ASSERT_EQ(Status::OK, delete_record(s, st, b));
    ASSERT_EQ(Status::OK, insert(s, st, b, b));
    ASSERT_EQ(Status::OK, delete_record(s, st, b));

    // verify
    // check by scan
    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));
    std::string buf{};
    // a
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, buf));
    ASSERT_EQ(buf, a);
    ASSERT_EQ(Status::OK, next(s, hd));
    // b
    ASSERT_EQ(Status::WARN_ALREADY_DELETE, read_key_from_scan(s, hd, buf));
    ASSERT_EQ(Status::OK, next(s, hd));
    // c
    ASSERT_EQ(Status::OK, read_key_from_scan(s, hd, buf));
    ASSERT_EQ(buf, c);
    ASSERT_EQ(Status::WARN_SCAN_LIMIT, next(s, hd));
    ASSERT_EQ(Status::OK, close_scan(s, hd));

    // check by search
    ASSERT_EQ(Status::OK, search_key(s, st, a, buf));
    ASSERT_EQ(Status::WARN_NOT_FOUND, search_key(s, st, b, buf));
    ASSERT_EQ(Status::OK, search_key(s, st, c, buf));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing