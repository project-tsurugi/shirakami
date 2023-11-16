
#include "concurrency_control/include/session.h"
#include "concurrency_control/include/tuple_local.h"
#include "database/include/logging.h"

#include "shirakami/api_diagnostic.h"
#include "shirakami/logging.h"

#include "glog/logging.h"


namespace shirakami {

void print_diagnostics(std::ostream& out) {
    shirakami_log_entry << "print_diagnostics";
    out << log_location_prefix << "print diagnostics start" << std::endl;

    // print for all session
    session_table::print_diagnostics(out);

    out << log_location_prefix << "print diagnostics end" << std::endl;
    shirakami_log_exit << "print_diagnostics";
}

} // namespace shirakami