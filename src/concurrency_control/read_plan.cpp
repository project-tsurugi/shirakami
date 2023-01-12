
#include <algorithm>
#include <set>

#include "concurrency_control/include/read_plan.h"
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "concurrency_control/include/wp.h"

#include "glog/logging.h"

namespace shirakami {

Status check_list(std::set<Storage> const& target_list,
                  std::set<wp::page_set_meta*>& out_list) {
    for (auto elem : target_list) {
        wp::page_set_meta* out{};
        auto rc = find_page_set_meta(elem, out);
        if (rc == Status::WARN_NOT_FOUND) { return Status::WARN_INVALID_ARGS; }
        // rc must be Status::OK
        if (rc != Status::OK) {
            LOG(ERROR) << log_location_prefix
                       << "It strongly suspect that DML and DDL are mixed.";
            return Status::ERR_FATAL;
        }
        out_list.insert(out);
    }

    return Status::OK;
}

Status check_storage_existence_and_collect_psm_info(
        transaction_options::read_area const& ra,
        std::set<wp::page_set_meta*>& plist_meta,
        std::set<wp::page_set_meta*>& nlist_meta) {
    // clear each out list
    plist_meta.clear();
    nlist_meta.clear();

    // about positive list
    auto rc = check_list(ra.get_positive_list(), plist_meta);
    if (rc != Status::OK) { return rc; }

    // about negative list
    rc = check_list(ra.get_negative_list(), nlist_meta);
    if (rc != Status::OK) { return rc; }

    return Status::OK;
}

void update_read_area(std::size_t const tx_id,
                      std::set<wp::page_set_meta*> const& plist_meta,
                      std::set<wp::page_set_meta*> const& nlist_meta) {
    // about plist
    for (auto* elem : plist_meta) {
        elem->get_read_plan().update_positive_list(tx_id);
    }

    // about nlist
    for (auto* elem : nlist_meta) {
        elem->get_read_plan().update_negative_list(tx_id);
    }
}

Status set_read_plans(Token token, std::size_t const tx_id,
                      transaction_options::read_area const& ra) {
    auto* ti = static_cast<session*>(token);

    // check storage existence and collect metadata info
    std::set<wp::page_set_meta*> plist_meta{};
    std::set<wp::page_set_meta*> nlist_meta{};
    auto rc = check_storage_existence_and_collect_psm_info(ra, plist_meta,
                                                           nlist_meta);
    if (rc != Status::OK) { return rc; }
    // success

    update_read_area(tx_id, plist_meta, nlist_meta);

    // log plist nlist
    ti->set_read_positive_list(plist_meta);
    ti->set_read_negative_list(nlist_meta);

    return Status::OK;
}

} // namespace shirakami