/**
 * @file session_info_table.cpp
 * @brief about entire shirakami.
 */

#include "concurrency_control/silo_variant/include/session_info_table.h"

#include "concurrency_control/silo_variant/include/garbage_collection.h"

#include "tuple_local.h"  // sizeof(Tuple)

namespace shirakami::cc_silo_variant {

Status session_info_table::decide_token(Token &token) {  // NOLINT
    for (auto &&itr : kThreadTable) {
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
    uint64_t ctr(0);
    for (auto &&itr : kThreadTable) {
        itr.set_visible(false);
        itr.set_tx_began(false);

        /**
         * about garbage collection.
         * note : the length of kGarbageRecords is KVS_NUMBER_OF_LOGICAL_CORES.
         * So it needs surplus operation.
         */
        std::size_t gc_index = ctr % KVS_NUMBER_OF_LOGICAL_CORES;
        itr.set_gc_container_index(gc_index);
        itr.set_gc_record_container(
                &garbage_collection::get_garbage_records_at(gc_index));
        itr.set_gc_value_container(
                &garbage_collection::get_garbage_values_at(gc_index));

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
#endif
        ++ctr;
    }
}

void session_info_table::fin_kThreadTable() {
    for (auto &&itr : kThreadTable) {
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
    }
}

}  // namespace shirakami::silo_variant
