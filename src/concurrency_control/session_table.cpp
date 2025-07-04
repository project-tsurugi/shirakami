
#include "include/session.h"

#include "shirakami/interface.h"

namespace shirakami {

Status session_table::decide_token(Token& token) { // NOLINT
    for (auto&& itr : get_session_table()) {
        if (!itr.get_visible()) {
            bool expected(false);
            bool desired(true);
            if (itr.cas_visible(expected, desired)) {
                token = static_cast<void*>(&itr);
                break;
            }
        }
        if (&itr == get_session_table().end() - 1) {
            return Status::ERR_SESSION_LIMIT;
        }
    }

    return Status::OK;
}

void session_table::init_session_table() {
    std::size_t worker_number = 0;
    for (auto&& itr : get_session_table()) {
        // for external
        itr.set_visible(false);
        // for internal
        itr.clean_up();
        // clear metadata about auto commit.
        itr.set_requested_commit(false);
        // for tx counter
        itr.set_higher_tx_counter(0);
        itr.set_tx_counter(0);
        itr.set_session_id(worker_number);
        // for commit callback
        itr.set_commit_callback({});
        // for mrc tid
        itr.set_mrc_tid(session::initial_mrc_tid);
        // for ltx
        itr.set_was_considering_forwarding_at_once(false);
#ifdef PWAL
        itr.get_lpwal_handle().init();
        itr.get_lpwal_handle().set_worker_number(worker_number);
#endif
        itr.clear_short_expose_ongoing_status();
        ++worker_number;
    }
}

static void check_and_update_diag_state(session* ti) {
    // long tx may remain waiting start state.
    if (ti->get_tx_type() != transaction_options::transaction_type::SHORT) {
        if (ti->get_diag_tx_state_kind() == TxState::StateKind::WAITING_START) {
            // Maybe started is more correct
            if (ti->get_valid_epoch() <= epoch::get_global_epoch()) {
                // started is more correct.
                ti->set_diag_tx_state_kind(TxState::StateKind::STARTED);
            }
        }
    }
}

static void display_tx_diagnostics_info(std::ostream& out, bool& exist_ltx,
                                        std::size_t& highest_priori_ltxid,
                                        session*& highest_priori_ltx_session,
                                        session* ti) {
    // output txid
    std::string buf{};
    get_tx_id(static_cast<Token>(ti), buf);
    out << "TID: " << buf << '\n';

    // check tx mode
    out << "tx mode: " << ti->get_tx_type() << '\n';

    // if ltx, ltx id
    if (ti->get_tx_type() == transaction_options::transaction_type::LONG) {
        out << "ltx id: " << ti->get_long_tx_id() << '\n';
        // analysis for ltx
        if (exist_ltx) {
            if (highest_priori_ltxid > ti->get_long_tx_id()) {
                highest_priori_ltxid = ti->get_long_tx_id();
                highest_priori_ltx_session = ti;
            }
        } else {
            // initialize
            exist_ltx = true;
            highest_priori_ltxid = ti->get_long_tx_id();
            highest_priori_ltx_session = ti;
        }
    }

    // update diag state if need
    check_and_update_diag_state(ti);
    // check this state
    TxState::StateKind st = ti->get_diag_tx_state_kind();
    out << "state: " << st << std::endl; // NOLINT(*-avoid-endl)
}

void session_table::print_diagnostics(std::ostream& out) {
    std::size_t num_running_tx{0};
    bool exist_ltx{false};
    std::size_t highest_priori_ltxid{0};
    session* highest_priori_ltx_session{nullptr};

    for (auto&& itr : get_session_table()) {
        // print diagnostics
        // check it was began
        if (!itr.get_tx_began()) { continue; } // not executing
        ++num_running_tx;

        // output start with token id
        out << "==token: " << &(itr) << ", start information\n";

        // print tx information for diagnostics
        display_tx_diagnostics_info(out, exist_ltx, highest_priori_ltxid,
                                    highest_priori_ltx_session, &itr);

        // output end with token id
        out << "==token: " << &(itr) << ", end information" << std::endl; // NOLINT(*-avoid-endl)
    }
    out << "number of running tx: " << num_running_tx << '\n';
    if (exist_ltx) {
        out << "highest priori LTX id was: " << highest_priori_ltxid << "\n"
               "re-display about the worker which has the tx: start\n";
        display_tx_diagnostics_info(out, exist_ltx, highest_priori_ltxid,
                                    highest_priori_ltx_session,
                                    highest_priori_ltx_session);
        out << "re-display about the worker which has the tx: end\n";
    }
    out << std::flush;
}

} // namespace shirakami
