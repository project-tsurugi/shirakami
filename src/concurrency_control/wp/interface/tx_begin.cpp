

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

Status tx_begin(transaction_options options) { // NOLINT
    Token token = options.get_token();
    transaction_options::transaction_type tx_type =
            options.get_transaction_type();
    transaction_options::write_preserve_type write_preserve =
            options.get_write_preserve();

    auto* ti = static_cast<session*>(token);
    ti->process_before_start_step();
    if (!ti->get_tx_began()) {
        if (!write_preserve.empty()) {
            if (tx_type != transaction_options::transaction_type::LONG) {
                return Status::WARN_ILLEGAL_OPERATION;
            }
        }
        if (tx_type == transaction_options::transaction_type::LONG) {
            auto rc{long_tx::tx_begin(ti, write_preserve,
                                      options.get_read_area())};
            if (rc != Status::OK) {
                ti->process_before_finish_step();
                return rc;
            }
            ti->get_write_set().set_for_batch(true);
        } else if (tx_type == transaction_options::transaction_type::SHORT) {
            ti->get_write_set().set_for_batch(false);
        } else if (tx_type ==
                   transaction_options::transaction_type::READ_ONLY) {
            auto rc{read_only_tx::tx_begin(ti)};
            if (rc != Status::OK) {
                LOG(ERROR) << rc;
                ti->process_before_finish_step();
                return rc;
            }
        } else {
            LOG(ERROR) << "programming error";
            return Status::ERR_FATAL;
        }
        ti->set_tx_type(tx_type);
        ti->set_tx_began(true);
    } else {
        ti->process_before_finish_step();
        return Status::WARN_ALREADY_BEGIN;
    }

    ti->process_before_finish_step();
    return Status::OK;
}

} // namespace shirakami