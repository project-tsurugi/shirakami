
#include <mutex>
#include <string>

#include "concurrency_control/include/session.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

#include "test_tool.h"

namespace shirakami::testing {

using namespace shirakami;

class insert_update_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "insert_update-insert_update_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
    }
    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(insert_update_test, insert_update) { // NOLINT
    // prepare
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));

    // test
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, st, "", "v"));
    ASSERT_EQ(Status::OK, update(s, st, "", "v1"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // verify
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    std::string buf{};
    ASSERT_EQ(Status::OK, search_key(s, st, "", buf));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(buf, "v1");

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(insert_update_test, insert_update_blob) {
    // prepare
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_OK(enter(s));

    // test and verify
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    const blob_id_type b1[2] = {11, 22};
    ASSERT_OK(insert(s, st, "", "v", b1, 2));
    {
        ASSERT_EQ(1, static_cast<session*>(s)->get_write_set().get_ref_cont_for_occ().size());
        auto& wso = static_cast<session*>(s)->get_write_set().get_ref_cont_for_occ().at(0);
        EXPECT_EQ(std::vector(std::begin(b1), std::end(b1)), wso.get_lobs());
    }
    const blob_id_type b2[3] = {99, 88, 77};
    ASSERT_OK(update(s, st, "", "v1", b2, 3));
    {
        ASSERT_EQ(1, static_cast<session*>(s)->get_write_set().get_ref_cont_for_occ().size());
        auto& wso = static_cast<session*>(s)->get_write_set().get_ref_cont_for_occ().at(0);
        EXPECT_EQ(std::vector(std::begin(b2), std::end(b2)), wso.get_lobs());
    }
    ASSERT_OK(commit(s));

    // cleanup
    ASSERT_OK(leave(s));
}

TEST_F(insert_update_test, update_insert) { // NOLINT
    // prepare
    Storage st{};
    create_storage("", st);
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, st, "", ""));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // test
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, update(s, st, "", "v"));
    ASSERT_EQ(Status::WARN_ALREADY_EXISTS, insert(s, st, "", "v1"));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT

    // verify
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    std::string buf{};
    ASSERT_EQ(Status::OK, search_key(s, st, "", buf));
    ASSERT_EQ(Status::OK, commit(s)); // NOLINT
    ASSERT_EQ(buf, "v");

    // cleanup
    ASSERT_EQ(Status::OK, leave(s));
}

} // namespace shirakami::testing
