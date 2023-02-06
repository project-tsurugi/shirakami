
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"

#include "shirakami/api_diagnostic.h"

namespace shirakami {

void print_diagnostics(std::ostream& out) {
    // print for all session
    session_table::print_diagnostics(out);
}

} // namespace shirakami