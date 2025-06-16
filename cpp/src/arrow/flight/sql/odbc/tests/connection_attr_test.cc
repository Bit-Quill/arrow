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
  // First, do unsupported attributes for set, then do unsupported attributes for get to make sure null is returned.


TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrAsyncDbcEventUnsupported) {
  this->connect();

#ifdef SQL_ATTR_ASYNC_DBC_EVENT
  SQLRETURN ret =
      SQLSetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_EVENT, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  // Driver Manager on Windows returns error code HY118
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY118);
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrAyncEnableUnsupported) {
  this->connect();

#ifdef SQL_ATTR_ASYNC_ENABLE
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ASYNC_ENABLE, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrAyncDbcPcCallbackUnsupported) {
  this->connect();

#ifdef SQL_ATTR_ASYNC_DBC_PCALLBACK
  SQLRETURN ret =
      SQLSetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_PCALLBACK, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrAyncDbcPcContextUnsupported) {
  this->connect();

#ifdef SQL_ATTR_ASYNC_DBC_PCONTEXT
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_PCONTEXT, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrAutoIpdReadOnly) {
  this->connect();

  // Verify read-only attribute cannot be set
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_AUTO_IPD, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY092);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrConnectionDeadReadOnly) {
  this->connect();

  // Verify read-only attribute cannot be set
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_CONNECTION_DEAD, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY092);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrDbcInfoTokenUnsupported) {
  this->connect();

#ifdef SQL_ATTR_DBC_INFO_TOKEN
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_DBC_INFO_TOKEN, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrEnlistInDtcUnsupported) {
  this->connect();

  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ENLIST_IN_DTC, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrOdbcCursorsDMOnly) {
  this->allocEnvConnHandles();

  // Verify DM-only attribute is settable via Driver Manager
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ODBC_CURSORS,
                                    reinterpret_cast<SQLPOINTER>(SQL_CUR_USE_DRIVER), 0);
  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }
  EXPECT_EQ(ret, SQL_SUCCESS);

  std::string connect_str = this->getConnectionString();
  this->connectWithString(connect_str);
  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrQuietModeReadOnly) {
  this->connect();

  // Verify read-only attribute cannot be set
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_QUIET_MODE, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY092);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrTraceDMOnly) {
  this->connect();

  // Verify DM-only attribute is settable via Driver Manager
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_TRACE,
                                    reinterpret_cast<SQLPOINTER>(SQL_OPT_TRACE_OFF), 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrTracefileDMOnly) {
  this->connect();

  // Verify DM-only attribute is handled by Driver Manager
  
  // Use placeholder value as we want the call to fail, or else
  // the driver manager will produce a trace file.
  std::wstring trace_file = L"invalid/file/path";
  std::vector<SQLWCHAR> trace_file0(trace_file.begin(), trace_file.end());
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_TRACEFILE, &trace_file0[0],
                                    static_cast<SQLINTEGER>(trace_file0.size()));
  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY000);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrTranslateLabDMOnly) {
  this->connect();

  // Verify DM-only attribute is handled by Driver Manager
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_TRANSLATE_LIB, 0, 0);
  EXPECT_EQ(ret, SQL_ERROR);
  // Checks for invalid argument return error
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY024);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrTranslateOptionUnsupported) {
  this->connect();

  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_TRANSLATE_OPTION, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrTxnIsolationUnsupported) {
  this->connect();

  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_TXN_ISOLATION,
                        reinterpret_cast<SQLPOINTER>(SQL_TXN_READ_UNCOMMITTED), 0);
  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->disconnect();
}

// Tests for supported attributes

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
