/**
 * @file
 * @brief operation about key
 * @author takayuki tanabe
 */

#include "key.hh"

void process_key(char** const key, std::size_t& len_key, std::string& newkey) {
  if (*key == nullptr) {
    return;
  }

  std::size_t delta_key = 8 - (len_key % 8);
  if (delta_key != 8) {
    newkey.assign(*key, len_key);
    newkey.append(std::string(delta_key, '\0'));
    *key = const_cast<char*>(newkey.c_str());
    len_key = newkey.size();
  }
}
