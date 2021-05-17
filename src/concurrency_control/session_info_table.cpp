/**
 * @file session_info_table.cpp
 * @brief about entire shirakami.
 */

#include "concurrency_control/include/session_info_table.h"
#include "concurrency_control/include/garbage_collection.h"

#include "tuple_local.h" // sizeof(Tuple)

#if defined(PWAL)

#include "log.h"

#endif

namespace shirakami {

Status session_info_table::decide_token(Token& token) { // NOLINT
    for (auto&& itr : kThreadTable) {
        if (!itr.get_visible()) {
            bool expected(false);
            bool desired(true);
            if (itr.cas_visible(expected, desired)) {
                token = static_cast<void*>(&(itr));
                break;
            }
        }
        if (&itr == kThreadTable.end() - 1) return Status::ERR_SESSION_LIMIT;
    }

    return Status::OK;
}

void session_info_table::init_kThreadTable() {
#if defined(PWAL)
    uint64_t ctr(0);
#endif
    for (auto&& itr : kThreadTable) {
        itr.set_visible(false);
        itr.set_tx_began(false);

        /**
         * about logging.
         */
#if defined(PWAL)
        std::string log_dir = Log::get_kLogDirectory();
        log_dir += "/log";
        log_dir.append(std::to_string(ctr));
        if (!itr.get_log_handler().get_log_file().open(log_dir, O_CREAT | O_TRUNC | O_WRONLY, 0644)) { // NOLINT
            std::cerr << __FILE__ << " : " << __LINE__ << " : error." << std::endl;
            exit(1);
        }
        // itr->log_file_.ftruncate(10^9); // if it want to be high performance in
        // experiments, this line is used.
        ++ctr;
#endif
#if defined(CPR)
        itr.reserve_diff_set(); // NOLINT
#endif
    }
}

void session_info_table::fin_kThreadTable() {
    for (auto&& itr : kThreadTable) {
        /**
         * about holding operation info.
         */
        itr.clean_up_ops_set();

        /**
         * about scan operation
         */
        itr.clean_up_scan_caches();

        /**
         * about logging
         */
#ifdef PWAL
        itr.get_log_set().clear();
        itr.get_log_handler().get_log_file().close();
#endif

#ifdef CPR
        itr.clear_diff_set();
#endif
    }
}

} // namespace shirakami
