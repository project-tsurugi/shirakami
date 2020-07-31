/**
 * @file scheme.cpp
 * @brief about scheme
 */

#include "cc/silo_variant/include/garbage_collection.h"
#include "cc/silo_variant/include/thread_info.h"
#include "include/tuple_local.h"

namespace shirakami::cc_silo_variant {

bool WriteSetObj::operator<(const WriteSetObj& right) const {  // NOLINT
  const Tuple& this_tuple = this->get_tuple(this->get_op());
  const Tuple& right_tuple = right.get_tuple(right.get_op());

  const char* this_key_ptr(this_tuple.get_key().data());
  const char* right_key_ptr(right_tuple.get_key().data());
  std::size_t this_key_size(this_tuple.get_key().size());
  std::size_t right_key_size(right_tuple.get_key().size());

  if (this_key_size < right_key_size) {
    return memcmp(this_key_ptr, right_key_ptr, this_key_size) <= 0;
  }

  if (this_key_size > right_key_size) {
    return memcmp(this_key_ptr, right_key_ptr, right_key_size) < 0;
  }
  int ret = memcmp(this_key_ptr, right_key_ptr, this_key_size);
  if (ret < 0) {
    return true;
  }
  if (ret > 0) {
    return false;
  }
  std::cout << __FILE__ << " : " << __LINE__
            << " : Unique key is not allowed now." << std::endl;
  std::abort();
}

void WriteSetObj::reset_tuple_value(const char* const val_ptr,
                                    const std::size_t val_length) & {
  (this->get_op() == OP_TYPE::UPDATE ? this->get_tuple_to_local()
                                     : this->get_tuple_to_db())
      .get_pimpl()
      ->set_value(val_ptr, val_length);
}

}  // namespace shirakami::silo_variant
