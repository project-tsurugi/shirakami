
#include "include/session.h"

#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/include/wp.h"

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
    wp_set_.clear();
    write_set_.clear();
    overtaken_ltx_set_.clear();
    if (tx_type_ != transaction_options::transaction_type::SHORT) {
        clear_about_read_area(this);
        set_read_area({});
    }
}

void session::clear_tx_property() { set_tx_began(false); }

std::set<std::size_t> session::extract_wait_for() {
    // extract wait for
    std::set<std::size_t> wait_for;
    for (auto&& each_pair : get_overtaken_ltx_set()) {
        for (auto&& each_id : each_pair.second) { wait_for.insert(each_id); }
    }
    return wait_for;
}

Status session::find_high_priority_short() const {
    if (get_tx_type() == transaction_options::transaction_type::SHORT) {
        LOG(ERROR) << "programming error";
        return Status::ERR_FATAL;
    }

    for (auto&& itr : session_table::get_session_table()) {
        if (itr.get_visible() &&
            itr.get_tx_type() == transaction_options::transaction_type::SHORT &&
            itr.get_operating() && itr.get_step_epoch() < get_valid_epoch()) {
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

void session::set_result(std::string_view str) {
    get_result_info().set_additional_information(str);
}

void session::set_result(reason_code rc, std::string_view str) {
    get_result_info().set_reason_code(rc);
    get_result_info().set_additional_information(str);
}

// ========== end: result info

} // namespace shirakami