

#include "concurrency_control/wp/include/local_set.h"
#include "concurrency_control/wp/include/record.h"
#include "concurrency_control/wp/include/session.h"
#include "concurrency_control/wp/interface/batch/include/batch.h"

#include "concurrency_control/include/tuple_local.h"

#include "index/yakushima/include/interface.h"

#include "yakushima/include/kvs.h"

namespace shirakami::batch {

Status update([[maybe_unused]] session* ti, [[maybe_unused]] Storage storage,
              [[maybe_unused]] const std::string_view key,
              [[maybe_unused]] const std::string_view val) {
    return Status::ERR_NOT_IMPLEMENTED;
}

} // namespace shirakami::batch