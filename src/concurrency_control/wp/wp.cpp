
#include "include/wp.h"

#include "shirakami/interface.h"

namespace shirakami {

Status wp::fin() {
    if (!get_initialized()) { return Status::WARN_NOT_INIT; }

    auto ret = delete_storage(get_page_set_meta_storage());
    if (ret != Status::OK) { return ret; }
    set_page_set_meta_storage(initial_page_set_meta_storage);
    set_initialized(false);
    return Status::OK;
}

Status wp::init() {
    if (get_initialized()) { return Status::WARN_ALREADY_INIT; }

    Storage ret_storage{};
    if (Status::OK != register_storage(ret_storage)) {
        return Status::ERR_STORAGE;
    }
    set_page_set_meta_storage(ret_storage);
    set_initialized(true);
    return Status::OK;
}

} // namespace shirakami