

#include "concurrency_control/wp/include/local_set.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/interface/batch/include/batch.h"

#include "concurrency_control/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

#include "yakushima/include/kvs.h"

namespace shirakami::batch {

Status update(session* ti, Storage storage, const std::string_view key,
              const std::string_view val) {
    return Status::ERR_NOT_IMPLEMENTED;
}

} // namespace shirakami::batch