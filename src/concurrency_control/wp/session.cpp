
#include "include/session.h"

namespace shirakami {

void session::clean_up_local_set() {
    read_set_.clear();
    write_set_.clear();
    node_set_.clear();
}

} // namespace shirakami