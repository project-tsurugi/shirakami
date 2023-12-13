#pragma once

#include <ostream>

namespace shirakami {
/**
     * @brief It prints diagnostics about transaction execute engine about below.
     * transaction state for all running tx. number of running tx. Waited 
     * transactions by living each LTX.
     * @param out 
     */
void print_diagnostics(std::ostream& out);

} // namespace shirakami