

#include <string_view>

#include "atomic_wrapper.h"
#include "storage.h"
#include "tsc.h"

#include "include/helper.h"

#include "concurrency_control/bg_work/include/bg_commit.h"
#include "concurrency_control/include/epoch_internal.h"
#include "concurrency_control/include/ongoing_tx.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"
#include "concurrency_control/interface/read_only_tx/include/read_only_tx.h"

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

void fin([[maybe_unused]] bool force_shut_down_logging) try {
    if (!get_initialized()) { return; }

    /**
     * about back ground worker about commit
     * background worker about commit may access global data (object), so it 
     * must execute before cleanup environment.
     */
    bg_work::bg_commit::fin();

    // about datastore
#if defined(PWAL)
    lpwal::fin(); // stop damon
    if (!force_shut_down_logging) {
        // flush remaining log
        lpwal::flush_remaining_log(); // (*1)
        epoch::epoch_t ce{epoch::get_global_epoch()};
        // wait durable above flushing
        for (;;) {
            if (epoch::get_durable_epoch() >= ce) { break; }
            _mm_pause();
        }
    }
    if (lpwal::get_log_dir_pointed()) {
        // create snapshot for next start.
        recover(datastore::get_datastore());
    }

#endif

    // about tx engine
    garbage::fin();
    epoch::fin();
#ifdef PWAL
    datastore::get_datastore()->shutdown(); // this should after epoch::fin();
    // cleanup about limestone
    if (!lpwal::get_log_dir_pointed()) {
        // log dir was not pointed. So remove log dir
        lpwal::remove_under_log_dir();
    }
    lpwal::clean_up_metadata();
#endif
    delete_all_records(); // This should be before wp::fin();
    wp::fin();            // note: this use yakushima.
    storage::fin();       // note: this use yakushima. delete meta storage.

    // about index
    yakushima::fin();

    // clear flag
    set_initialized(false);
} catch (std::exception& e) {
    LOG(ERROR) << e.what();
    return;
}

} // namespace shirakami