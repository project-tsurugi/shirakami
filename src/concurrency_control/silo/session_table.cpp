/**
 * @file session_table.cpp
 * @brief about entire shirakami.
 */

#include "include/session_table.h"
#include "include/storage.h"
#include "include/tuple_local.h" // sizeof(Tuple)

#if defined(PWAL)

#include "log.h"

#endif

#include "glog/logging.h"

namespace shirakami {

Status session_table::decide_token(Token& token) { // NOLINT
    for (auto&& itr : get_session_table()) {
        if (!itr.get_visible()) {
            bool expected(false);
            bool desired(true);
            if (itr.cas_visible(expected, desired)) {
                token = static_cast<void*>(&(itr));
                break;
            }
        }
        if (&itr == get_session_table().end() - 1) {
            return Status::ERR_SESSION_LIMIT;
        }
    }

    return Status::OK;
}

#ifdef CPR
void session_table::display_diff_set() {
    LOG(INFO) << "session_table::display_diff_set";
    for (auto&& elem : get_session_table()) { elem.display_diff_set(); }
}
#endif

void session_table::init_session_table([[maybe_unused]] bool enable_recovery) {
#if defined(PWAL)
    uint64_t ctr(0);
#endif
    for (auto&& itr : get_session_table()) {
        itr.set_visible(false);
        itr.set_tx_began(false);

        /**
         * about logging.
         */
#if defined(PWAL)
        std::string log_dir = Log::get_kLogDirectory();
        log_dir += "/log";
        log_dir.append(std::to_string(ctr));
        if (!itr.get_log_handler().get_log_file().open(
                    log_dir, O_CREAT | O_TRUNC | O_WRONLY, 0644)) { // NOLINT
            std::cerr << __FILE__ << " : " << __LINE__ << " : error."
                      << std::endl;
            exit(1);
        }
        // itr->log_file_.ftruncate(10^9); // if it want to be high performance in
        // experiments, this line is used.
        ++ctr;
#endif
#if defined(CPR)
        itr.clear_diff_set();
        //itr.reserve_diff_set(); // NOLINT
#endif
    }
}

void session_table::fin_session_table() {
    std::vector<std::thread> th_vc;
    th_vc.reserve(get_session_table().size());
    for (auto&& itr : get_session_table()) {
        auto process = [&itr]() {
            /**
              * about holding operation info.
              */
            itr.clean_up_local_set();

            /**
              * about scan operation
              */
            itr.clean_up_scan_caches();

            /**
             *  cleanup manager
             */
            itr.get_cleanup_handle().clear();

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
        };
#ifdef CPR
        if (itr.get_diff_upd_set(0).size() > 1000 ||             // NOLINT
            itr.get_diff_upd_set(1).size() > 1000 ||             // NOLINT
            itr.get_cleanup_handle().get_cont().size() > 1000) { // NOLINT
            // Considering clean up time of test and benchmark.
            th_vc.emplace_back(process);
        } else {
            process();
        }
#else
        process();
#endif
    }

    for (auto&& th : th_vc) th.join();
}

#ifdef CPR

bool session_table::is_empty_logs() {
    for (auto&& elem : session_table::get_session_table()) {
        if (!elem.diff_upd_set_is_empty() ||
            !elem.diff_upd_seq_set_is_empty()) {
            return false;
        }
    }
    return true;
}

#endif

} // namespace shirakami
