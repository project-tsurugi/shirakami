
#include "storage.h"

#include "concurrency_control/wp/include/wp.h"

#include "concurrency_control/include/tuple_local.h"

#include "shirakami/interface.h"

#include "yakushima/include/kvs.h"

namespace shirakami {

[[maybe_unused]] Status delete_all_records() { // NOLINT
    std::vector<Storage> storage_list;
    list_storage(storage_list);
    for (auto&& elem : storage_list) {
        if (elem != wp::get_page_set_meta_storage()) {
            storage::delete_storage(elem);
        }
    }

    return Status::OK;
}

Status delete_record([[maybe_unused]] Token token,
                     [[maybe_unused]] Storage storage,
                     [[maybe_unused]] const std::string_view key) { // NOLINT
    return Status::ERR_NOT_IMPLEMENTED;
}

} // namespace shirakami