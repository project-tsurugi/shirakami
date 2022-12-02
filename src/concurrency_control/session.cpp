
#include "sequence.h"
#include "storage.h"

#include "include/session.h"

#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"

#include "shirakami/log_record.h"

#include "glog/logging.h"

namespace shirakami {

bool session::check_exist_wp_set(Storage storage) const {
    for (auto&& elem : get_wp_set()) {
        if (elem.first == storage) { return true; }
    }
    return false;
}

void clear_about_read_area(session* ti) {
    // gc global information
    if (!ti->get_read_positive_list().empty()) {
        for (auto* elem : ti->get_read_positive_list()) {
            elem->get_read_plan().erase_positive_list(ti->get_long_tx_id());
        }
    }
    if (!ti->get_read_negative_list().empty()) {
        for (auto* elem : ti->get_read_negative_list()) {
            elem->get_read_plan().erase_negative_list(ti->get_long_tx_id());
        }
    }

    // clear plist nlist
    ti->get_read_negative_list().clear();
    ti->get_read_positive_list().clear();
}

void session::clear_local_set() {
    node_set_.clear();
    point_read_by_long_set_.clear();
    range_read_by_long_set_.clear();
    point_read_by_short_set_.clear();
    range_read_by_short_set_.clear();
    read_set_.clear();
    read_set_for_ltx().clear();
    wp_set_.clear();
    write_set_.clear();
    overtaken_ltx_set_.clear();
    if (tx_type_ != transaction_options::transaction_type::SHORT) {
        clear_about_read_area(this);
        set_read_area({});
    }
    sequence_set().clear();
}

void session::clear_tx_property() { set_tx_began(false); }

void session::commit_sequence(tid_word ctid) {
    auto& ss = sequence_set().set();
    // ss is sequence set
    if (!ss.empty()) {

        std::lock_guard<std::shared_mutex> lk{sequence::sequence_map_smtx()};

        // gc after write lock
        sequence::gc_sequence_map();

        for (auto itr = ss.begin(); itr != ss.end();) {
            SequenceId id = itr->first;
            SequenceVersion version = std::get<0>(itr->second);
            SequenceValue value = std::get<1>(itr->second);

            // check update sequence object
            auto ret = sequence::sequence_map_find_and_verify(id, version);
            if (ret == Status::WARN_ILLEGAL_OPERATION ||
                ret == Status::WARN_NOT_FOUND) {
                // fail
                itr = ss.erase(itr);
                continue;
            }
            if (ret != Status::OK) {
                LOG(ERROR) << "programming error";
                itr = ss.erase(itr);
                continue;
            }

#ifdef PWAL
            // This entry is valid. it generates log.
            // gen key
            std::string key{};
            key.append(reinterpret_cast<const char*>(&id), // NOLINT
                       sizeof(id));
            // gen value
            std::string new_value{}; // value is version + value
            new_value.append(reinterpret_cast<const char*>(&version), // NOLINT
                             sizeof(version));
            new_value.append(reinterpret_cast<const char*>(&value), // NOLINT
                             sizeof(value));
            log_operation lo{log_operation::UPSERT};
            get_lpwal_handle().push_log(shirakami::lpwal::log_record(
                    lo, lpwal::write_version_type(ctid.get_epoch(), version),
                    storage::sequence_storage, key, new_value));
#endif

            // update sequence object
            auto epoch = ctid.get_epoch();
            ret = sequence::sequence_map_update(id, epoch, version, value);
            if (ret != Status::OK) {
                LOG(ERROR) << "programming error";
                return;
            }
        }
    }
}

std::set<std::size_t> session::extract_wait_for() {
    // extract wait for
    std::set<std::size_t> wait_for;
    for (auto&& each_pair : get_overtaken_ltx_set()) {
        for (auto&& each_id : std::get<0>(each_pair.second)) {
            wait_for.insert(each_id);
        }
    }
    return wait_for;
}

Status session::find_high_priority_short() const {
    if (get_tx_type() == transaction_options::transaction_type::SHORT) {
        LOG(ERROR) << "programming error";
        return Status::ERR_FATAL;
    }

    for (auto&& itr : session_table::get_session_table()) {
        if (
                // already enter
                itr.get_visible() &&
                // short tx
                itr.get_tx_type() ==
                        transaction_options::transaction_type::SHORT &&
                itr.get_operating() &&
                /**
                 * If operating false and this ltx can start in the viewpoint
                 * of epoch, stx after this operation must be serialized after 
                 * this ltx.
                 */
                // transaction order
                itr.get_step_epoch() < get_valid_epoch()) {
            return Status::WARN_PREMATURE;
        }
    }
    return Status::OK;
}

Status session::find_wp(Storage st) const {
    for (auto&& elem : get_wp_set()) {
        if (elem.first == st) { return Status::OK; }
    }
    return Status::WARN_NOT_FOUND;
}

// ========== start: result info
void session::set_result(reason_code rc) {
    get_result_info().set_reason_code(rc);
    get_result_info().clear_additional_information();
}

void session::set_result(reason_code rc, std::string_view str) {
    get_result_info().set_reason_code(rc);
    get_result_info().set_additional_information(str);
}

// ========== end: result info

} // namespace shirakami