
#include <mutex>
#include <vector>

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

class tsurugi_issue234 : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-concurrency_control-"
                                  "complicated-tsurugi_issue234");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        std::call_once(init_, call_once_f);
        init(); // NOLINT
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(tsurugi_issue234, comment_by_ban_20230308) { // NOLINT
    /**
     * テストシナリオ
     * ltx scan が最新のバージョンを読むと楽観的に判断し、楽観検証を実施したときに
     * タイムスタンプのずれを観測してロックフリーにバージョン走査をする。そのバージョン
     * 走査の結果が ver = nullptr であるときがありそう。
     * 
     * コードロジック的には最新のバージョンが読めると楽観的に判断した時点で、楽観検証
     * に失敗してもバージョンリストの中にその読めるものが存在するはずなので、それを
     * 見つけられないことはバージョン走査に関するバグである。
     */
    Storage st{};
    ASSERT_EQ(create_storage("", st), Status::OK);
    Token s1{};
    Token s2{};
    ASSERT_EQ(Status::OK, enter(s1));
    ASSERT_EQ(Status::OK, enter(s2));

    // create page x
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(insert(s1, st, "x", "v1"), Status::OK);
    ASSERT_EQ(Status::OK, commit(s1));

    // tx begin s1
    ASSERT_EQ(Status::OK,
              tx_begin({s1, transaction_options::transaction_type::LONG}));
    wait_epoch_update();
    // it can see x-v1

    ScanHandle hd{};
    ASSERT_EQ(Status::OK, open_scan(s1, st, "", scan_endpoint::INF, "",
                                    scan_endpoint::INF, hd));

    ASSERT_EQ(Status::OK,
              tx_begin({s2, transaction_options::transaction_type::SHORT}));
    ASSERT_EQ(Status::OK, upsert(s2, st, "x", "v2"));

    /**
     * 必要な手順
     * 1. s1 が最新バージョンを読めるものとして楽観的に判断する。(sleep1)
     * 2. s2 がロックを取ることでタイムスタンプが変化する。(sleep1)
     * 3. s1 が楽観検証で 2 による（ロックビットによる）タイムスタンプ変化を観測して
     * 楽観検証に失敗したとして、失敗したならば次以降のバージョンが読めるものがあるは
     * ずと判断する。
     * 4. 3 でその次のバージョンを探しても nullptr である。
     */
    auto s1_work = [s1, hd]() {
        std::string buf{};
        ASSERT_EQ(Status::OK, read_value_from_scan(s1, hd, buf));
        ASSERT_EQ(buf, "v1");
    };

    auto s2_work = [s2]() {
        // write lock を取った後に sleep(1) を挟む
        ASSERT_EQ(Status::OK, commit(s2));
    };

    auto th_1 = std::thread(s1_work);
    auto th_2 = std::thread(s2_work);

    // cleanup
    th_1.join();
    th_2.join();
    ASSERT_EQ(Status::OK, commit(s1));
    ASSERT_EQ(Status::OK, leave(s1));
    ASSERT_EQ(Status::OK, leave(s2));
}

} // namespace shirakami::testing