// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cassert>

#include <arrow/flight/sql/odbc/tests/odbc_test_suite.h>

namespace arrow {
namespace flight {
namespace odbc {
namespace integration_tests {

std::string SqlWcharToString(SQLWCHAR* wchar_msg) {
  if (wchar_msg == nullptr) {
    return std::string();
  }

  // Size of SQLWCHAR depends on the platform
  size_t wchar_size = sizeof(SQLWCHAR);

  // Assert that size of SQLWCHAR and wchar_t are the same
  assert(wchar_size == sizeof(wchar_t));

  // Copy SQLWCHAR into a temp wstring
  std::wstring wstring_msg;

  for (int i = 0; wchar_msg[i] != 0; i++) {
    wstring_msg.push_back(wchar_msg[i]);
  }

  // Converter for SQLWCHAR to wstring
  static std::wstring_convert<std::codecvt_utf8<wchar_t>> conv;
  return conv.to_bytes(wstring_msg);
}

std::string GetOdbcErrorMessage(SQLSMALLINT handle_type, SQLHANDLE handle) {
  SQLWCHAR sql_state[7] = {};
  SQLINTEGER native_code;

  SQLWCHAR message[ODBC_BUFFER_SIZE] = {};
  SQLSMALLINT reallen = 0;

  // On Windows, reallen is in bytes. On Linux, reallen is in chars.
  // So, not using reallen
  SQLGetDiagRec(handle_type, handle, 1, sql_state, &native_code, message,
  ODBC_BUFFER_SIZE, &reallen);

  std::string res = SqlWcharToString(sql_state);

  if (res.empty() || !message[0]) {
  res = "Cannot find ODBC error message";
  } else {
  res.append(": ").append(SqlWcharToString(message));
  }

  return res;
}

// -AL- todo potentially add `connectToFlightSql` here.
}  // namespace integration_tests
}  // namespace odbc
}  // namespace flight
}  // namespace arrow
