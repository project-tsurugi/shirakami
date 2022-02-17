
#include "concurrency_control/wp/include/local_set.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/session.h"

#include "concurrency_control/wp/interface/occ/include/occ.h"

#include "concurrency_control/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

namespace shirakami::occ {

Status update(session* ti, Storage storage, const std::string_view key,
              const std::string_view val) {
    return Status::ERR_NOT_IMPLEMENTED;
}

} // namespace shirakami::occ