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
#include "arrow/flight/sql/odbc/tests/odbc_test_suite.h"

#ifdef _WIN32
#  include <windows.h>
#endif

#include <sql.h>
#include <sqltypes.h>
#include <sqlucode.h>

#include "gtest/gtest.h"

namespace arrow {
namespace flight {
namespace odbc {
namespace integration_tests {
  // -AL- add connection attribute tests in this file.

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrUnsupported) {
  this->connect();

  SQLUINTEGER uint = -1;

  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_CONNECTION_TIMEOUT, &uint, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  //VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_28000);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrTimeoutValid) {
  this->connect();

  SQLUINTEGER timeout = -1;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_CONNECTION_TIMEOUT, &timeout, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(timeout, 0);

  ret = SQLSetConnectAttr(this->conn, SQL_ATTR_CONNECTION_TIMEOUT,
                          reinterpret_cast<SQLPOINTER>(42), 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  timeout = -1;

  ret = SQLGetConnectAttr(this->conn, SQL_ATTR_CONNECTION_TIMEOUT, &timeout, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(timeout, 42);

  this->disconnect();
}

}  // namespace integration_tests
}  // namespace odbc
}  // namespace flight
}  // namespace arrow
