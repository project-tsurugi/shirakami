/**
 * @file interface_delete.cpp
 * @brief implement about transaction
 */

#include <bitset>

#include "atomic_wrapper.h"

#include "concurrency_control/silo_variant/include/garbage_collection.h"
#include "concurrency_control/silo_variant/include/interface_helper.h"

#include "kvs/interface.h"

// sizeof(Tuple)

namespace shirakami::cc_silo_variant {

[[maybe_unused]] Status delete_all_records() {  // NOLINT
    std::vector<std::pair<Record**, std::size_t> > scan_res;
    yakushima::scan("", yakushima::scan_endpoint::INF, "", yakushima::scan_endpoint::INF, scan_res); // NOLINT

    for (auto &&itr : scan_res) {
        delete *itr.first;  // NOLINT
    }

    yakushima::destroy();

    return Status::OK;
}

Status delete_record(Token token, const std::string_view key) { // NOLINT
    auto* ti = static_cast<session_info*>(token);
    if (!ti->get_txbegan()) tx_begin(token); // NOLINT
    Status check = ti->check_delete_after_write(key);

    Record** rec_double_ptr{yakushima::get<Record*>(key).first};
    if (rec_double_ptr == nullptr) {
        return Status::WARN_NOT_FOUND;
    }
    Record* rec_ptr{*rec_double_ptr};
    tid_word check_tid(loadAcquire(rec_ptr->get_tidw().get_obj()));
    if (check_tid.get_absent()) {
        // The second condition checks
        // whether the record you want to read should not be read by parallel
        // insert / delete.
        return Status::WARN_NOT_FOUND;
    }

    ti->get_write_set().emplace_back(OP_TYPE::DELETE, rec_ptr);
    return check;
}

}  // namespace shirakami::cc_silo_variant
