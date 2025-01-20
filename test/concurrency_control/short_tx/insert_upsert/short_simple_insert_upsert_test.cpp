
#include <mutex>
#include <string>

#include "concurrency_control/include/session.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

#include "test_tool.h"


namespace shirakami::testing {

using namespace shirakami;

Storage st{};
class simple_insert_upsert : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-silo-"
                                  "upsert-simple_insert_upsert_test");
        // FLAGS_stderrthreshold = 0; // output more than INFO
    }
    void SetUp() override {
        std::call_once(init_google_, call_once_f);
        init(); // NOLINT
        create_storage("", st);
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_google_; // NOLINT
};

TEST_F(simple_insert_upsert, upsert_after_insert) { // NOLINT
    std::string k("K");
    std::string v("v");
    Token s{};
    ASSERT_EQ(Status::OK, enter(s));
    ASSERT_EQ(Status::OK,
              tx_begin({s, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, insert(s, st, k, v));
    ASSERT_EQ(Status::OK, upsert(s, st, k, v));
    ASSERT_EQ(Status::OK, commit(s));
    ASSERT_EQ(Status::OK, leave(s));
}

TEST_F(simple_insert_upsert, upsert_after_insert_blob) {
    std::string k("K");
    Token s{};
    ASSERT_OK(enter(s));
    ASSERT_OK(tx_begin({s, transaction_options::transaction_type::SHORT}));
    const blob_id_type b1[2] = {11, 22};
    ASSERT_OK(insert(s, st, k, "v1", b1, 2));
    {
        ASSERT_EQ(1, static_cast<session*>(s)->get_write_set().get_ref_cont_for_occ().size());
        auto& wso = static_cast<session*>(s)->get_write_set().get_ref_cont_for_occ().at(0);
        EXPECT_EQ("v1", wso.get_value_view());
        EXPECT_EQ(std::vector(std::begin(b1), std::end(b1)), wso.get_lobs());
    }
    const blob_id_type b2[3] = {99, 88, 77};
    ASSERT_OK(upsert(s, st, k, "v2", b2, 3));
    {
        ASSERT_EQ(1, static_cast<session*>(s)->get_write_set().get_ref_cont_for_occ().size());
        auto& wso = static_cast<session*>(s)->get_write_set().get_ref_cont_for_occ().at(0);
        EXPECT_EQ("v2", wso.get_value_view());
        EXPECT_EQ(std::vector(std::begin(b2), std::end(b2)), wso.get_lobs());
    }
    ASSERT_OK(commit(s));
    ASSERT_OK(leave(s));
}

} // namespace shirakami::testing
