
#include "include/tuple_local.h"

#include "shirakami/tuple.h"

namespace shirakami {

std::string_view Tuple::get_key() const {
    return pimpl_->get_key();
}

std::string_view Tuple::get_value() const {
    return pimpl_->get_val();
}

} // namespace shirakami