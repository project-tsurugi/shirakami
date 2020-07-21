/**
 * @file tid.cc
 * @details implement about tid.
 */

#include "tid.h"

#include <bitset>
#include <iostream>

using namespace kvs;

using std::cout, std::endl;

namespace kvs {

void TidWord::display() {
  cout << "obj_ : " << std::bitset<sizeof(obj_) * 8>(obj_) << endl;  // NOLINT
  cout << "lock_ : " << lock_ << endl;                               // NOLINT
  cout << "latest_ : " << latest_ << endl;                           // NOLINT
  cout << "absent_ : " << absent_ << endl;                           // NOLINT
  cout << "tid_ : " << tid_ << endl;                                 // NOLINT
  cout << "epoch_ : " << epoch_ << endl;                             // NOLINT
}

}  // namespace kvs
