#include "gtest/gtest.h"

#include "kvs/scheme.h"
#include "scheme.hh"

using namespace kvs;
using std::cout;
using std::endl;

namespace shirakami::testing {

class schemeTest : public ::testing::Test {
protected:
  schemeTest() {}
  ~schemeTest() {}
};

// kvs/scheme.h

TEST_F(schemeTest, to_string_view_Status) {
  using namespace std::string_view_literals;
  Status status = Status::WARN_ALREADY_DELETE;
  ASSERT_EQ("WARN_ALREADY_DELETE"sv, to_string_view(status));
  status = Status::WARN_ALREADY_EXISTS;
  ASSERT_EQ("WARN_ALREADY_EXISTS"sv, to_string_view(status));
  status = Status::WARN_CANCEL_PREVIOUS_OPERATION;
  ASSERT_EQ("WARN_CANCEL_PREVIOUS_OPERATION"sv, to_string_view(status));
  status = Status::WARN_CONCURRENT_DELETE;
  ASSERT_EQ("WARN_CONCURRENT_DELETE"sv, to_string_view(status));
  status = Status::WARN_INVALID_HANDLE;
  ASSERT_EQ("WARN_INVALID_HANDLE"sv, to_string_view(status));
  status = Status::WARN_NOT_FOUND;
  ASSERT_EQ("WARN_NOT_FOUND"sv, to_string_view(status));
  status = Status::WARN_NOT_IN_A_SESSION;
  ASSERT_EQ("WARN_NOT_IN_A_SESSION"sv, to_string_view(status));
  status = Status::WARN_READ_FROM_OWN_OPERATION;
  ASSERT_EQ("WARN_READ_FROM_OWN_OPERATION"sv, to_string_view(status));
  status = Status::WARN_SCAN_LIMIT;
  ASSERT_EQ("WARN_SCAN_LIMIT"sv, to_string_view(status));
  status = Status::WARN_WRITE_TO_LOCAL_WRITE;
  ASSERT_EQ("WARN_WRITE_TO_LOCAL_WRITE"sv, to_string_view(status));
  status = Status::OK;
  ASSERT_EQ("OK"sv, to_string_view(status));
  status = Status::ERR_INVALID_ARGS;
  ASSERT_EQ("ERR_INVALID_ARGS"sv, to_string_view(status));
  status = Status::ERR_NOT_FOUND;
  ASSERT_EQ("ERR_NOT_FOUND"sv, to_string_view(status));
  status = Status::ERR_SESSION_LIMIT;
  ASSERT_EQ("ERR_SESSION_LIMIT"sv, to_string_view(status));
  status = Status::ERR_VALIDATION;
  ASSERT_EQ("ERR_VALIDATION"sv, to_string_view(status));
  status = Status::ERR_WRITE_TO_DELETED_RECORD;
  ASSERT_EQ("ERR_WRITE_TO_DELETED_RECORD"sv, to_string_view(status));
  status = (Status)INT32_MAX;
  // below statements occur std::abort()
#if 0
  try {
    to_string_view(status);
  } catch (...) {
  }
#endif
}

TEST_F(schemeTest, ostream_operator_Status) { cout << Status::OK << endl; }

TEST_F(schemeTest, to_string_view_OP_TYPE) {
  using namespace std::string_view_literals;
  OP_TYPE op;
  op = OP_TYPE::ABORT;
  ASSERT_EQ("ABORT"sv, to_string_view(op));
  op = OP_TYPE::BEGIN;
  ASSERT_EQ("BEGIN"sv, to_string_view(op));
  op = OP_TYPE::COMMIT;
  ASSERT_EQ("COMMIT"sv, to_string_view(op));
  op = OP_TYPE::DELETE;
  ASSERT_EQ("DELETE"sv, to_string_view(op));
  op = OP_TYPE::INSERT;
  ASSERT_EQ("INSERT"sv, to_string_view(op));
  op = OP_TYPE::NONE;
  ASSERT_EQ("NONE"sv, to_string_view(op));
  op = OP_TYPE::SEARCH;
  ASSERT_EQ("SEARCH"sv, to_string_view(op));
  op = OP_TYPE::UPDATE;
  ASSERT_EQ("UPDATE"sv, to_string_view(op));
}

TEST_F(schemeTest, ostream_operator_OP_TYPE) {
  cout << OP_TYPE::SEARCH << endl;
}

// scheme.hh

}  // namespace shirakami::testing
