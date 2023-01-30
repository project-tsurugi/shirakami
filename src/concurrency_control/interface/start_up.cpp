

#include <string_view>

#include "atomic_wrapper.h"
#include "sequence.h"
#include "storage.h"
#include "tsc.h"

#include "include/helper.h"

#include "concurrency_control/bg_work/include/bg_commit.h"
#include "concurrency_control/include/epoch_internal.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"
#include "concurrency_control/interface/read_only_tx/include/read_only_tx.h"

#include "database/include/logging.h"

#ifdef PWAL

#include "concurrency_control/include/lpwal.h"

#include "datastore/limestone/include/datastore.h"
#include "datastore/limestone/include/limestone_api_helper.h"

#include "limestone/api/datastore.h"

#endif

#include "index/yakushima/include/interface.h"

#include "shirakami/interface.h"

#include "boost/filesystem/path.hpp"

#include "glog/logging.h"

namespace shirakami {

Status init(database_options const options) { // NOLINT
    if (get_initialized()) { return Status::WARN_ALREADY_INIT; }

    // about logging detail information
    logging::init(options.get_enable_logging_detail_info());

    // about storage
    storage::init();

    /**
     * about sequence. the generator of sequence id is cleared, So if this is 
     * a start up with recovery, it must also recovery sequence id generator.
     */
    sequence::init();

#if defined(PWAL)
    // check args
    std::string log_dir(options.get_log_directory_path());
    bool enable_true_log_nothing{false};
    if (log_dir.empty()) {
        if (options.get_open_mode() == database_options::open_mode::RESTORE) {
            // order to recover, but log_dir is nothing
            enable_true_log_nothing = true;
        }
        int tid = syscall(SYS_gettid); // NOLINT
        std::uint64_t tsc = rdtsc();
        log_dir = "/tmp/shirakami-" + std::to_string(tid) + "-" +
                  std::to_string(tsc);
        lpwal::set_log_dir_pointed(false);
        lpwal::set_log_dir(log_dir);
    } else {
        lpwal::set_log_dir(log_dir);
        lpwal::set_log_dir_pointed(true);
        // check exist
        boost::filesystem::path ldp{
                std::string(options.get_log_directory_path())};
        boost::system::error_code error;
        const bool result = boost::filesystem::exists(ldp, error);
        if (!result || error) {
            // exists
            if (options.get_open_mode() ==
                database_options::open_mode::CREATE) {
                // there are some data not expected.
                lpwal::set_log_dir(log_dir);
                lpwal::remove_under_log_dir();
            }
        }
    }

    // start datastore
    std::string data_location_str(log_dir);
    boost::filesystem::path data_location(data_location_str);
    std::vector<boost::filesystem::path> data_locations;
    data_locations.emplace_back(data_location);
    std::string metadata_dir{log_dir + "m"};
    boost::filesystem::path metadata_path(metadata_dir);
    try {
        datastore::start_datastore(
                limestone::api::configuration(data_locations, metadata_path));
    } catch (...) { return Status::ERR_INVALID_CONFIGURATION; }
    if (options.get_open_mode() != database_options::open_mode::CREATE &&
        !enable_true_log_nothing) {
        recover(datastore::get_datastore());
    }
    datastore::get_datastore()->add_persistent_callback(
            epoch::set_durable_epoch); // should execute before ready()
    /**
     * This executes create_channel and pass it to shirakami's executor.
     */
    datastore::init_about_session_table(log_dir);
    ready(datastore::get_datastore());

#endif

    // about tx state
    TxState::init();

    // about cc
    session_table::init_session_table();
    //epoch::invoke_epoch_thread();

    // about index
    // pre condition : before wp::init() because wp::init() use yakushima function.
    yakushima::init();

    // about wp
    auto ret = wp::init();
    if (ret != Status::OK) { return ret; }

    // about meta storage
    storage::init_meta_storage();

#ifdef PWAL
    // recover shirakami from datastore recovered.
    if (options.get_open_mode() != database_options::open_mode::CREATE &&
        !enable_true_log_nothing) {
        datastore::recovery_from_datastore();
        // logging the shirakami state after recovery
        datastore::scan_all_and_logging(); // todo remove?
    }
#endif

    // about epoch
    epoch::init();
    garbage::init();

#ifdef PWAL
    lpwal::init(); // start damon
#endif

    // about back ground worker about commit
    bg_work::bg_commit::init();

    set_initialized(true); // about init command
    return Status::OK;
}

} // namespace shirakami