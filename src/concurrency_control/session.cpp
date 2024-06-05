
#include "sequence.h"
#include "storage.h"

#include "include/session.h"

#include "concurrency_control/include/wp.h"

#include "shirakami/log_record.h"

#include "glog/logging.h"

namespace shirakami {

bool session::check_exist_wp_set(Storage storage) const {
    return std::any_of(get_wp_set().begin(), get_wp_set().end(),
                       [storage](std::pair<Storage, wp::wp_meta*> elem) {
                           return elem.first == storage;
                       });
}

void clear_about_read_area(session* ti) { ti->set_read_area({}); }

void session::clear_local_set() {
    node_set_.clear();
    get_range_read_set_for_ltx().clear();
    clear_ltx_storage_read_set();
    clear_range_read_by_short_set();
    clear_read_set_for_stx();
    read_set_for_ltx().clear();
    wp_set_.clear();
    write_set_.clear();
    clear_overtaken_ltx_set();
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
    if (ss.empty()) { return; }

    std::lock_guard<std::shared_mutex> lk_for_sm{sequence::sequence_map_smtx()};

    // gc after write lock
    sequence::gc_sequence_map();

#ifdef PWAL
    std::vector<shirakami::lpwal::log_record> log_recs{};
#endif
    for (auto itr = ss.begin(); itr != ss.end();) { // NOLINT
        SequenceId id = itr->first;
        SequenceVersion version = std::get<0>(itr->second);
        SequenceValue value = std::get<1>(itr->second);
        // check update sequence object
        auto ret = sequence::sequence_map_find_and_verify(ctid.get_epoch(), id,
                                                          version);
        if (ret == Status::WARN_ILLEGAL_OPERATION ||
            ret == Status::WARN_NOT_FOUND) {
            // fail
            itr = ss.erase(itr);
            continue;
        }
        if (ret != Status::OK) {
            LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
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
        // log to local to reduce contention for locks
        log_recs.emplace_back(shirakami::lpwal::log_record(
                lo, lpwal::write_version_type(ctid.get_epoch(), version),
                storage::sequence_storage, key, new_value));
#endif

        // update sequence object
        auto epoch = ctid.get_epoch();
        ret = sequence::sequence_map_update(id, epoch, version, value);
        if (ret != Status::OK) {
            LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
            return;
        }
        ++itr;
    }

#ifdef PWAL
    // push log to pwal buffer
    std::unique_lock<std::mutex> lk_for_pwal_buf{
            get_lpwal_handle().get_mtx_logs()};
    for (auto& elem : log_recs) { get_lpwal_handle().push_log(elem); }
#endif
}

std::set<std::size_t> session::extract_wait_for() {
    // extract wait for
    std::set<std::size_t> wait_for;
    {
        // get read lock
        std::shared_lock<std::shared_mutex> lk{
                this->get_mtx_overtaken_ltx_set()};
        for (auto&& each_pair : get_overtaken_ltx_set()) {
            for (auto&& each_id : std::get<0>(each_pair.second)) {
                wait_for.insert(each_id);
            }
        }
    }
    return wait_for;
}

Status session::find_high_priority_short(bool for_check) const {
    if (get_tx_type() == transaction_options::transaction_type::SHORT) {
        LOG_FIRST_N(ERROR, 1) << log_location_prefix << "unreachable path";
        return Status::ERR_FATAL;
    }

    for (auto&& itr : session_table::get_session_table()) {
        if (
                // already enter
                itr.get_visible() &&
                // short tx
                itr.get_tx_type() ==
                        transaction_options::transaction_type::SHORT &&
                (itr.get_operating().load(std::memory_order_acquire) != 0) &&
                itr.get_tx_began() &&
                /**
                 * If operating false and this ltx can start in the viewpoint
                 * of epoch, stx after this operation must be serialized after 
                 * this ltx.
                 */
                // transaction order
                itr.get_step_epoch() < get_valid_epoch()) {
            // logging
            if (VLOG_IS_ON(for_check ? log_debug : log_error)) {
                std::string str_ltx_id{};
                std::string str_stx_id{};
                get_tx_id(static_cast<Token>(const_cast<session*>(this)),
                          str_ltx_id);
                get_tx_id(static_cast<Token>(const_cast<session*>(&itr)),
                          str_stx_id);
                LOG(INFO) << log_location_prefix
                          << "ltx warn premature by short tx, ltx id: "
                          << str_ltx_id << ", stx id: " << str_stx_id;

            }
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
}

// ========== end: result info

} // namespace shirakami