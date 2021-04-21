
#include "concurrency_control//include/interface_helper.h"
#include "concurrency_control/include/session_info_table.h"
#include "concurrency_control/include/snapshot_interface.h"

#include "tuple_local.h"

#include "shirakami/interface.h"

namespace shirakami {

Status search_key(Token token, Storage storage, const std::string_view key,  // NOLINT
                  Tuple** const tuple) {
    auto* ti = static_cast<session_info*>(token);
    if (!ti->get_txbegan()) {
        tx_begin(token); // NOLINT
    } else if (ti->get_read_only()) {
        return snapshot_interface::lookup_snapshot(ti, storage, key, tuple);
    }

    write_set_obj* inws{ti->search_write_set(key)};
    if (inws != nullptr) {
        if (inws->get_op() == OP_TYPE::DELETE) {
            return Status::WARN_ALREADY_DELETE;
        }
        *tuple = &inws->get_tuple(inws->get_op());
        return Status::WARN_READ_FROM_OWN_OPERATION;
    }

    Record** rec_double_ptr{std::get<0>(yakushima::get<Record*>({reinterpret_cast<char*>(&storage), sizeof(storage)}, key))}; // NOLINT
    if (rec_double_ptr == nullptr) {
        *tuple = nullptr;
        return Status::WARN_NOT_FOUND;
    }
    Record* rec_ptr{*rec_double_ptr};

    read_set_obj rs_ob(rec_ptr); // NOLINT
    Status rr = read_record(rs_ob.get_rec_read(), rec_ptr);
    if (rr == Status::OK) {
        ti->get_read_set().emplace_back(std::move(rs_ob));
        *tuple = &ti->get_read_set().back().get_rec_read().get_tuple();
    } else {
        *tuple = nullptr;
    }
    return rr;
}

}  // namespace shirakami
