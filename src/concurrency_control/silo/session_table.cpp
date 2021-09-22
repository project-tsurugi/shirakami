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
        itr.reserve_diff_set(); // NOLINT
#endif
    }

#if defined(CPR)
    if (enable_recovery) {
        // log recovered records.
        std::vector<Storage> storage_list;
        storage::list_storage(storage_list); // NOLINT
        for (auto&& storage : storage_list) {
            std::vector<std::tuple<std::string, Record**, std::size_t>>
                    scan_buf;
            std::string_view storage_view = {
                    reinterpret_cast<char*>(&storage), // NOLINT
                    sizeof(storage)};
            yakushima::scan(storage_view, "", yakushima::scan_endpoint::INF, "",
                            yakushima::scan_endpoint::INF, scan_buf);
            for (auto&& elem : scan_buf) {
                auto& map =
                        get_session_table().at(0).get_diff_upd_set(); // NOLINT
                auto* record = *std::get<1>(elem);
                map[std::string(storage_view)]
                   [std::string(record->get_tuple().get_key())] =
                           std::make_tuple(0, false,
                                           record->get_tuple().get_value());
            }
        }
    }
#endif
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
