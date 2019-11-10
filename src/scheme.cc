
#include "include/scheme.h"

namespace kvs{

ReadSetObj* ThreadInfo::search_read_set(const char* key, std::size_t len_key)
{
  for (auto itr = read_set.begin(); itr != read_set.end(); ++itr) {
    if ((*itr).rec_ptr->tuple.len_key == len_key
        && memcmp((*itr).rec_ptr->tuple.key.get(), key, len_key) == 0) {
      return &(*itr);
    }
  }
  return nullptr;
}

WriteSetObj* ThreadInfo::search_write_set(const char* key, std::size_t len_key)
{
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    if ((*itr).rec_ptr->tuple.len_key == len_key
        && memcmp((*itr).rec_ptr->tuple.key.get(), key, len_key) == 0) {
      return &(*itr);
    }
  }
  return nullptr;
}

WriteSetObj* ThreadInfo::search_write_set(const char* key, std::size_t len_key, OP_TYPE op)
{
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    if ((*itr).rec_ptr->tuple.len_key == len_key
        && (*itr).op == op
        && memcmp((*itr).rec_ptr->tuple.key.get(), key, len_key) == 0) {
      return &(*itr);
    }
  }
  return nullptr;
}

}
