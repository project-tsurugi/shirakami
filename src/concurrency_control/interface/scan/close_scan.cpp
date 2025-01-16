
#include "atomic_wrapper.h"

#include "concurrency_control/include/helper.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/wp_meta.h"
#include "concurrency_control/interface/include/helper.h"
#include "concurrency_control/interface/long_tx/include/long_tx.h"
#include "concurrency_control/interface/short_tx/include/short_tx.h"
#include "database/include/logging.h"
#include "index/yakushima/include/interface.h"
#include "index/yakushima/include/scheme.h"


#include "shirakami/binary_printer.h"
#include "shirakami/interface.h"

#include "glog/logging.h"

namespace shirakami {

Status close_scan_body(Token const token, ScanHandle const handle) { // LINT
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }
    return ti->get_scan_handle().clear(handle);
}

Status close_scan(Token const token, ScanHandle const handle) { // LINT
    shirakami_log_entry << "close_scan, token: " << token
                        << ", handle: " << handle;
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    Status ret{};
    { // for strand
        std::shared_lock<std::shared_mutex> lock{ti->get_mtx_state_da_term()};
        ret = close_scan_body(token, handle);
    }
    ti->process_before_finish_step();
    shirakami_log_exit << "close_scan, Status: " << ret;
    return ret;
}

} // namespace shirakami
