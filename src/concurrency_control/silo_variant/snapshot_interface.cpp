//
// Created by thawk on 2021/01/19.
//

#include "kvs/tuple.h"

#include "concurrency_control/silo_variant/include/snapshot_interface.h"
#include "concurrency_control/silo_variant/include/session_info.h"

using namespace shirakami;
using namespace cc_silo_variant;

namespace shirakami::cc_silo_variant::snapshot_interface {

Status lookup_snapshot(Token token, std::string_view key, Tuple** const ret_tuple) {
    auto* ti = static_cast<session_info*>(token);

    Record** rec_d_ptr{std::get<0>(yakushima::get<Record*>(key))};
    if (rec_d_ptr == nullptr) {
        // There is no record which has the key.
        *ret_tuple = nullptr;
        return Status::WARN_NOT_FOUND;
    }

    Record* rec_ptr{*rec_d_ptr};
    tid_word tid = loadAcquire(rec_ptr->get_tidw().get_obj());

READ_PROCESS_START:
    // phase 1 : decide to see main record or snapshot.
    // phase 1-1 : wait releasing lock
    if (tid.get_lock()) {
        do {
            _mm_pause();
            tid = loadAcquire(rec_ptr->get_tidw().get_obj());
        } while (tid.get_lock());
    }

    // phase 1-2 : check snapshot epoch
    if (epoch::get_snap_epoch(ti->get_epoch()) != epoch::get_snap_epoch(tid.get_epoch())) {
        // it should read from main record.
    } else {
        // it should read from snapshot.
        if (rec_ptr->get_snap_ptr() != nullptr) {
            // there is a snapshot to see.
            Tuple escape_tuple = rec_ptr->get_snap_ptr()->get_tuple();
            if (tid == loadAcquire(rec_ptr->get_tidw().get_obj())) {
                // success atomic read
                ti->get_read_only_tuples().emplace_back(std::move(escape_tuple));
                *ret_tuple = &ti->get_read_only_tuples().back();
                return Status::OK;
            }
            // fail atomic read
            goto READ_PROCESS_START; // NOLINT
        } else {
            // This record was inserted at same snap(epoch), so there is no snapshot.
            return Status::WARN_NOT_FOUND;
        }
    }

    if (rec_ptr->get_snap_ptr() == nullptr) {
        // case : There is no snapshot to see.
        if (tid.get_lock()) {

        } else {

        }
        if (epoch::get_snap_epoch(tid.epoch_))
            return Status::WARN_NOT_FOUND;
    } else {
        // case : There is no snapshot to see.
    }
    return Status::OK;
}

}
