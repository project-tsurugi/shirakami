
#include "include/atomic_wrapper.hh"
#include "include/scheme.hh"

using std::cout;
using std::endl;

namespace kvs{

Status ThreadInfo::check_delete_after_upsert(const char* key, const std::size_t len_key)
{
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    if ((*itr).rec_ptr->tuple.len_key == len_key
        && memcmp((*itr).rec_ptr->tuple.key.get(), key, len_key) == 0) {
      write_set.erase(itr);
      return Status::WARN_CANCEL_PREVIOUS_OPERATION;
    }
  }

  return Status::OK;
}

void ThreadInfo::remove_inserted_records_of_write_set_from_masstree()
{
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    if ((*itr).op == OP_TYPE::INSERT) {
      MTDB.remove_value((*itr).rec_ptr->tuple.key.get(), (*itr).rec_ptr->tuple.len_key);
    }
  }
}

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

ReadSetObj* ThreadInfo::search_read_set(Record* rec_ptr)
{
  for (auto itr = read_set.begin(); itr != read_set.end(); ++itr)
    if ((*itr).rec_ptr == rec_ptr) return &(*itr);

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

WriteSetObj* ThreadInfo::search_write_set(Record* rec_ptr)
{
  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr)
    if ((*itr).rec_ptr == rec_ptr) return &(*itr);

  return nullptr;
}

void ThreadInfo::unlock_write_set() {
  TidWord expected, desired;

  for (auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    expected.obj = loadAcquire(itr->rec_ptr->tidw.obj);
    desired = expected;
    desired.lock = 0;
    storeRelease(itr->rec_ptr->tidw.obj, desired.obj);
  }
}

void ThreadInfo::unlock_write_set(std::vector<WriteSetObj>::iterator begin, std::vector<WriteSetObj>::iterator end) {
  TidWord expected, desired;

  for (auto itr = begin; itr != end; ++itr) {
    expected.obj = loadAcquire(itr->rec_ptr->tidw.obj);
    desired = expected;
    desired.lock = 0;
    storeRelease(itr->rec_ptr->tidw.obj, desired.obj);
  }
}

void print_status(Status status)
{
  switch (status) {
    case Status::WARN_ALREADY_IN_A_SESSION:
      cout << "WARN_ALREADY_IN_A_SESSION" << endl;
      break;
    case Status::WARN_NOT_IN_A_SESSION:
      cout << "WARN_NOT_IN_A_SESSION" << endl;
      break;
    case Status::OK:
      cout << "OK" << endl;
      break;
    case Status::ERR_UNKNOWN:
      cout << "ERR_UNKNOWN" << endl;
      break;
    case Status::ERR_NOT_FOUND:
      cout << "ERR_NOT_FOUND" << endl;
      break;
    case Status::ERR_ALREADY_EXISTS:
      cout << "ERR_ALREADY_EXISTS" << endl;
      break;
    case Status::ERR_INVALID_ARGS:
      cout << "ERR_INVALID_ARGS" << endl;
      break;
    case Status::ERR_ILLEGAL_STATE:
      cout << "ERR_ILLEGAL_STATE" << endl;
      break;
    case Status::ERR_VALIDATION:
      cout << "ERR_VALIDATION" << endl;
      break;
    default:
      cout << "UNKNWON_STATUS" << endl;
      break;
  }
}

}
