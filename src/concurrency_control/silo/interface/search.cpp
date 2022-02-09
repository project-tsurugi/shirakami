
#include "include/helper.h"

#include "concurrency_control/silo/include/session_table.h"
#include "concurrency_control/silo/include/snapshot_interface.h"

#include "concurrency_control/include/tuple_local.h" // sizeof(Tuple)

#include "shirakami/interface.h"

namespace shirakami {

Status exist_key(Token token, Storage storage, std::string_view const key) {
    auto* ti = static_cast<session*>(token);

    // check flags
    if (!ti->get_txbegan()) {
        tx_begin(token); // NOLINT
    } else if (ti->get_read_only()) {
        return snapshot_interface::lookup_snapshot(ti, storage, key);
    }

    // index access
    Record** rec_double_ptr{std::get<0>(yakushima::get<Record*>(
            {reinterpret_cast<char*>(&storage), sizeof(storage)}, // NOLINT
            key))};                                               // NOLINT
    if (rec_double_ptr == nullptr) { return Status::WARN_NOT_FOUND; }
    Record* rec_ptr{*rec_double_ptr};

    // check read own write
    write_set_obj* inws{ti->get_write_set().search(rec_ptr)}; // NOLINT
    if (inws != nullptr) {
        if (inws->get_op() == OP_TYPE::DELETE) {
            return Status::WARN_ALREADY_DELETE;
        }
        return Status::OK;
    }

    // data access
    read_set_obj rs_ob(storage, rec_ptr); // NOLINT
    tid_word tidb{};
    std::string keyb{};
    std::string dummy_valueb{};
    Status rr = read_record(rec_ptr, tidb, keyb, dummy_valueb, false);
    if (rr == Status::OK) {
        rs_ob.get_rec_read().set_tidw(tidb);
        rs_ob.get_rec_read().set_key(keyb);
        ti->get_read_set().emplace_back(std::move(rs_ob));
    }
    return rr;
}

Status search_key(Token token, Storage storage,
                  const std::string_view key, // NOLINT
                  std::string& value) {
    auto* ti = static_cast<session*>(token);

    // check flags
    if (!ti->get_txbegan()) {
        tx_begin(token); // NOLINT
    } else if (ti->get_read_only()) {
        return snapshot_interface::lookup_snapshot(ti, storage, key, value);
    }

    // index access
    Record** rec_double_ptr{std::get<0>(yakushima::get<Record*>(
            {reinterpret_cast<char*>(&storage), sizeof(storage)}, // NOLINT
            key))};                                               // NOLINT
    if (rec_double_ptr == nullptr) { return Status::WARN_NOT_FOUND; }
    Record* rec_ptr{*rec_double_ptr};

    // check read own write
    write_set_obj* inws{ti->get_write_set().search(rec_ptr)}; // NOLINT
    if (inws != nullptr) {
        if (inws->get_op() == OP_TYPE::DELETE) {
            return Status::WARN_ALREADY_DELETE;
        }
        inws->get_tuple(inws->get_op()).get_value(value);
        return Status::WARN_READ_FROM_OWN_OPERATION;
    }

    // data access
    read_set_obj rs_ob(storage, rec_ptr); // NOLINT
    tid_word tidb{};
    std::string keyb{};
    std::string valueb{};
    Status rr = read_record(rec_ptr, tidb, keyb, valueb);
    if (rr == Status::OK) {
        rs_ob.get_rec_read().set_tidw(tidb);
        rs_ob.get_rec_read().set_key(keyb);
        rs_ob.get_rec_read().set_value(valueb);
        ti->get_read_set().emplace_back(std::move(rs_ob));
        ti->get_read_set().back().get_rec_read().get_tuple().get_value(value);
    }
    return rr;
}

} // namespace shirakami
