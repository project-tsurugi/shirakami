

#include <string_view>

#include "atomic_wrapper.h"
#include "storage.h"
#include "tsc.h"

#include "include/helper.h"

#include "concurrency_control/wp/include/epoch_internal.h"
#include "concurrency_control/wp/include/ongoing_tx.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/wp.h"
#include "concurrency_control/wp/interface/long_tx/include/long_tx.h"
#include "concurrency_control/wp/interface/read_only_tx/include/read_only_tx.h"

#ifdef PWAL

#include "concurrency_control/wp/include/lpwal.h"

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

    // about datastore
#if defined(PWAL)
    lpwal::fin(); // stop damon
    if (!force_shut_down_logging) {
        // flush remaining log
        bool was_nothing{false};
        lpwal::flush_remaining_log(was_nothing); // (*1)
        epoch::epoch_t ce{epoch::get_global_epoch()};
        // (*1)'s log must be before ce timing.
        if (!was_nothing) {
            // wait durable above flushing
            for (;;) {
                if (epoch::get_durable_epoch() >= ce) { break; }
                _mm_pause();
            }
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
    LOG(FATAL) << e.what();
    std::abort();
}

} // namespace shirakami