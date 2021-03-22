/**
 * @file scheme.cpp
 * @brief about scheme
 */

#include "concurrency_control/silo_variant/include/garbage_collection.h"
#include "concurrency_control/silo_variant/include/session_info.h"
#include "tuple_local.h"

namespace shirakami {

bool write_set_obj::operator<(const write_set_obj &right) const {  // NOLINT
    return this->get_rec_ptr() < right.get_rec_ptr();
}

void write_set_obj::reset_tuple_value(std::string_view val) {
    (this->get_op() == OP_TYPE::UPDATE ? this->get_tuple_to_local()
                                       : this->get_tuple_to_db())
            .get_pimpl()
            ->set_value(val.data(), val.size());
}

}  // namespace shirakami::silo_variant
