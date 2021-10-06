
#include "include/session.h"

namespace shirakami {

void session::clean_up_local_set() {
    node_set_.clear();
    read_set_.clear();
    wp_set_.clear();
    write_set_.clear();
}

} // namespace shirakami