
#include <glog/logging.h>
#include <ostream>

#include "database/include/logging.h"
#include "datastore/limestone/include/datastore.h"
#include "shirakami/interface.h"

namespace shirakami {

void* get_datastore() {
    shirakami_log_entry << "get_datastore";
    auto* ret = static_cast<void*>(datastore::get_datastore());
    shirakami_log_exit << "get_datastore";
    return ret;
}

} // namespace shirakami