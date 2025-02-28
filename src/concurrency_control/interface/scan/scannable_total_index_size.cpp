
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

Status scannable_total_index_size_body(Token const token, ScanHandle const handle, // NOLINT
                                       std::size_t& size) {
    auto* ti = static_cast<session*>(token);
    if (!ti->get_tx_began()) { return Status::WARN_NOT_BEGIN; }

    auto& sh = ti->get_scan_handle();

    {
        std::shared_lock<std::shared_mutex> lk{sh.get_mtx_scan_cache()};
        if (sh.get_scan_cache().find(handle) == sh.get_scan_cache().end()) {
            /**
             * the handle was invalid.
             */
            return Status::WARN_INVALID_HANDLE;
        }

        size = sh.get_scan_cache()[handle].get_vec().size();
    }
    return Status::OK;
}

Status scannable_total_index_size(Token const token, ScanHandle const handle, // NOLINT
                                  std::size_t& size) {
    shirakami_log_entry << "scannable_total_index_size, "
                        << "token: " << token << ", handle: " << handle
                        << ", size: " << size;
    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    Status ret{};
    { // for strand
        std::shared_lock<std::shared_mutex> lock{ti->get_mtx_state_da_term()};
        ret = scannable_total_index_size_body(token, handle, size);
    }
    ti->process_before_finish_step();
    shirakami_log_exit << "scannable_total_index_size, Status: " << ret
                       << ", size: " << size;
    return ret;
}

} // namespace shirakami
