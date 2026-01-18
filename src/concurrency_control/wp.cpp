
#include <algorithm>
#include <string_view>
#include <vector>

#include "storage.h"

#include "concurrency_control/include/session.h"
#include "concurrency_control/include/wp.h"

#include "database/include/logging.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "glog/logging.h"

using namespace shirakami;

namespace shirakami::wp {

Status fin() {
    if (!get_initialized()) { return Status::WARN_NOT_INIT; }

    set_finalizing(true);
    Storage storage = get_page_set_meta_storage();
    auto rc = delete_storage(storage);
    if (Status::OK != rc) {
        LOG_FIRST_N(ERROR, 1)
                << log_location_prefix << rc << "unreachable path.";
        return Status::ERR_FATAL;
    }
    set_page_set_meta_storage(initial_page_set_meta_storage);
    set_initialized(false);
    set_finalizing(false);
    return Status::OK;
}

Status find_page_set_meta(Storage st, page_set_meta*& ret) {
    Storage page_set_meta_storage = get_page_set_meta_storage();
    std::string_view page_set_meta_storage_view = {
            reinterpret_cast<char*>(&page_set_meta_storage), // NOLINT
            sizeof(page_set_meta_storage)};
    std::string_view storage_view = {
            reinterpret_cast<const char*>(&st), // NOLINT
            sizeof(st)};
    std::pair<page_set_meta**, std::size_t> out{};
    auto rc{yakushima::get<page_set_meta*>(page_set_meta_storage_view, // NOLINT
                                           storage_view, out)};
    if (rc != yakushima::status::OK) {
        ret = nullptr;
        return Status::WARN_NOT_FOUND;
    }
    ret = reinterpret_cast<page_set_meta*>(out.first); // NOLINT
    // by inline optimization
    return Status::OK;
}

Status init() {
    if (get_initialized()) { return Status::WARN_ALREADY_INIT; }

    if (auto rc{storage::register_storage(storage::wp_meta_storage)};
        rc != Status::OK) {
        LOG_FIRST_N(ERROR, 1)
                << log_location_prefix << rc << ", unreachable path.";
    }
    set_page_set_meta_storage(storage::wp_meta_storage);
    set_initialized(true);
    return Status::OK;
}

} // namespace shirakami::wp
