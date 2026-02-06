
#include <mutex>
#include <memory>

#include "shirakami/interface.h"
#include "gtest/gtest.h"
#include "glog/logging.h"

#ifdef PWAL
#include "limestone/api/datastore.h"
#endif

#include "shirakami/api_diagnostic.h"
#include "shirakami/database_options.h"
#include "shirakami/scheme.h"

namespace shirakami::testing {

using namespace shirakami;

class start_test : public ::testing::Test { // NOLINT
public:
    static void call_once_f() {
        google::InitGoogleLogging("shirakami-test-start-start_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_google, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_google;
};

TEST_F(start_test, double_start) { // NOLINT
    ASSERT_EQ(init(), Status::OK);
    ASSERT_EQ(init(), Status::WARN_ALREADY_INIT);
    fin();
}

TEST_F(start_test, valid_recovery_invalid_log_directory) { // NOLINT
    ASSERT_EQ(init({database_options::open_mode::RESTORE, ""}), Status::OK);
    fin();
}

#ifdef PWAL

static limestone::api::configuration create_limestone_config(const std::string& path) {
#if HAVE_LIMESTONE_CONFIG_CTOR_NONE && HAVE_LIMESTONE_CONFIG_SET_DATA_LOCATION_STDFSPATH
    auto limestone_config = limestone::api::configuration{};
    limestone_config.set_data_location(path);
#else
    auto limestone_config = limestone::api::configuration({path}, path + "m");
#endif
    return limestone_config;
}

TEST_F(start_test, borrow_datastore) { // NOLINT
    auto limestone_config = create_limestone_config("/tmp/shirakamitest");
    auto datastore = new limestone::api::datastore(limestone_config);
    // transitional: need log_directory_path for now
    ASSERT_EQ(init({database_options::open_mode::RESTORE, "/tmp/shirakamitest"}, datastore), Status::OK);
    (void)datastore->last_epoch(); // check (by ASAN): datastore is not freed
    fin();
    (void)datastore->last_epoch(); // check (by ASAN): datastore is not freed
    delete datastore;
}

TEST_F(start_test, error_when_borrow_ds_and_maintenance) { // NOLINT
    auto limestone_config = create_limestone_config("/tmp/shirakamitest");
    auto datastore = new limestone::api::datastore(limestone_config);
    ASSERT_EQ(init({database_options::open_mode::MAINTENANCE, "/tmp/shirakamitest"}, datastore), Status::ERR_INVALID_CONFIGURATION);
    (void)datastore->last_epoch(); // check (by ASAN): datastore is not freed
    delete datastore;
}

#endif

} // namespace shirakami::testing
