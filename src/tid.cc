/**
 * @file tid.cc
 * @details implement about tid.
 */

#include <bitset>
#include <iostream>

#include "tid.hh"

using namespace kvs;

using std::cout, std::endl;

namespace  kvs {

void
TidWord::display()
{
  cout << "obj_ : " << std::bitset<sizeof(obj_)*8>(obj_) << endl;
  cout << "lock_ : " << lock_ << endl;
  cout << "latest_ : " << latest_ << endl;
  cout << "absent_ : " << absent_ << endl;
  cout << "tid_ : " << tid_ << endl;
  cout << "epoch_ : " << epoch_ << endl;
}

} // namespace kvs
