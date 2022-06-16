
#include "concurrency_control/wp/include/tuple_local.h"
#include "concurrency_control/wp/interface/long_tx/include/long_tx.h"
#include "concurrency_control/wp/interface/read_only_tx/include/read_only_tx.h"

#include "index/yakushima/include/interface.h"

namespace shirakami::read_only_tx {

Status search_key(session* ti, Storage const storage,
                  std::string_view const key,
                  [[maybe_unused]] std::string& value,
                  [[maybe_unused]] bool const read_value) {
    if (epoch::get_global_epoch() < ti->get_valid_epoch()) {
        return Status::WARN_PREMATURE;
    }
    if (ti->find_high_priority_short() == Status::WARN_PREMATURE) {
        return Status::WARN_PREMATURE;
    }

    // index access
    Record* rec_ptr{};
    if (Status::WARN_NOT_FOUND == get<Record>(storage, key, rec_ptr)) {
        return Status::WARN_NOT_FOUND;
    }

    return long_tx::version_traverse_and_read(ti, rec_ptr, value, read_value);
}

} // namespace shirakami::read_only_tx