
#include "datastore/limestone/include/datastore.h"

#include "shirakami/interface.h"

namespace shirakami {

void* get_datastore() { return static_cast<void*>(datastore::get_datastore()); }

} // namespace shirakami