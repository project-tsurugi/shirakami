

#include "include/wp.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

#include "glog/logging.h"

namespace shirakami {

Status wp::fin() {
    if (!get_initialized()) { return Status::WARN_NOT_INIT; }

    set_finalizing(true);
    Storage storage = get_page_set_meta_storage();
    auto rc = delete_storage(storage);
    if (Status::OK != rc) {
        LOG(FATAL) << rc;
        std::abort();
    }
    set_page_set_meta_storage(initial_page_set_meta_storage);
    set_initialized(false);
    set_finalizing(false);
    return Status::OK;
}

Status wp::init() {
    if (get_initialized()) { return Status::WARN_ALREADY_INIT; }

    Storage ret_storage{};
    auto rc = register_storage(ret_storage);
    if (Status::OK != rc) {
        LOG(FATAL) << rc;
        std::abort();
    }
    set_page_set_meta_storage(ret_storage);
    set_initialized(true);
    return Status::OK;
}

} // namespace shirakami