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

namespace arrow::flight::sql::odbc {

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetDiagFieldWForConnectFailure) {
  //  ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Invalid connect string
  std::string connect_str = this->getInvalidConnectionString();

  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR outstr[ODBC_BUFFER_SIZE];
  SQLSMALLINT outstrlen;

  // Connecting to ODBC server.
  ret = SQLDriverConnect(conn, NULL, &connect_str0[0],
                         static_cast<SQLSMALLINT>(connect_str0.size()), outstr,
                         ODBC_BUFFER_SIZE, &outstrlen, SQL_DRIVER_NOPROMPT);

  EXPECT_TRUE(ret == SQL_ERROR);

  // Retrieve all supported header level and record level data
  SQLSMALLINT HEADER_LEVEL = 0;
  SQLSMALLINT RECORD_1 = 1;

  // SQL_DIAG_NUMBER
  SQLINTEGER diag_number;
  SQLSMALLINT diag_number_length;

  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, HEADER_LEVEL, SQL_DIAG_NUMBER, &diag_number,
                        sizeof(SQLINTEGER), &diag_number_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(diag_number, 1);

  // SQL_DIAG_SERVER_NAME
  SQLWCHAR server_name[ODBC_BUFFER_SIZE];
  SQLSMALLINT server_name_length;

  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, RECORD_1, SQL_DIAG_SERVER_NAME, server_name,
                        ODBC_BUFFER_SIZE, &server_name_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // SQL_DIAG_MESSAGE_TEXT
  SQLWCHAR message_text[ODBC_BUFFER_SIZE];
  SQLSMALLINT message_text_length;

  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, RECORD_1, SQL_DIAG_MESSAGE_TEXT,
                        message_text, ODBC_BUFFER_SIZE, &message_text_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(message_text_length, 100);

  // SQL_DIAG_NATIVE
  SQLINTEGER diag_native;
  SQLSMALLINT diag_native_length;

  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, RECORD_1, SQL_DIAG_NATIVE, &diag_native,
                        sizeof(diag_native), &diag_native_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(diag_native, 200);

  // SQL_DIAG_SQLSTATE
  const SQLSMALLINT sql_state_size = 6;
  SQLWCHAR sql_state[sql_state_size];
  SQLSMALLINT sql_state_length;
  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, RECORD_1, SQL_DIAG_SQLSTATE, sql_state,
                        sql_state_size * driver::odbcabstraction::GetSqlWCharSize(),
                        &sql_state_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(std::wstring(sql_state), std::wstring(L"28000"));

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetDiagFieldWForConnectFailureNTS) {
  // Test is disabled because driver manager on Windows does not pass through SQL_NTS
  // This test case can be potentially used on macOS/Linux
  GTEST_SKIP();
  //  ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Invalid connect string
  std::string connect_str = this->getInvalidConnectionString();

  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR outstr[ODBC_BUFFER_SIZE];
  SQLSMALLINT outstrlen;

  // Connecting to ODBC server.
  ret = SQLDriverConnect(conn, NULL, &connect_str0[0],
                         static_cast<SQLSMALLINT>(connect_str0.size()), outstr,
                         ODBC_BUFFER_SIZE, &outstrlen, SQL_DRIVER_NOPROMPT);

  EXPECT_TRUE(ret == SQL_ERROR);

  // Retrieve all supported header level and record level data
  SQLSMALLINT RECORD_1 = 1;

  // SQL_DIAG_MESSAGE_TEXT SQL_NTS
  SQLWCHAR message_text[ODBC_BUFFER_SIZE];
  SQLSMALLINT message_text_length;

  message_text[ODBC_BUFFER_SIZE - 1] = '\0';

  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, RECORD_1, SQL_DIAG_MESSAGE_TEXT,
                        message_text, SQL_NTS, &message_text_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(message_text_length, 100);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetDiagRecForConnectFailure) {
  //  ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Invalid connect string
  std::string connect_str = this->getInvalidConnectionString();

  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR outstr[ODBC_BUFFER_SIZE];
  SQLSMALLINT outstrlen;

  // Connecting to ODBC server.
  ret = SQLDriverConnect(conn, NULL, &connect_str0[0],
                         static_cast<SQLSMALLINT>(connect_str0.size()), outstr,
                         ODBC_BUFFER_SIZE, &outstrlen, SQL_DRIVER_NOPROMPT);

  EXPECT_TRUE(ret == SQL_ERROR);

  SQLWCHAR sql_state[6];
  SQLINTEGER native_error;
  SQLWCHAR message[ODBC_BUFFER_SIZE];
  SQLSMALLINT message_length;

  ret = SQLGetDiagRec(SQL_HANDLE_DBC, conn, 1, sql_state, &native_error, message,
                      ODBC_BUFFER_SIZE, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(message_length, 120);

  EXPECT_EQ(native_error, 200);

  EXPECT_EQ(std::wstring(sql_state), std::wstring(L"28000"));

  EXPECT_TRUE(!std::wstring(message).empty());

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

// -AL- todo check for returns. Separate input tests from actual tests with values.
TYPED_TEST(FlightSQLODBCTestBase, TestSQLErrorInputData) {
  // Test ODBC 2.0 API SQLError. Driver manager maps SQLError to SQLGetDiagRec.
  // SQLError does not post diagnostic records for itself.
  this->connect();

  SQLRETURN ret = SQLError(this->env, 0, 0, 0, 0, 0, 0, 0);

  EXPECT_EQ(ret, SQL_NO_DATA);

  ret = SQLError(0, this->conn, 0, 0, 0, 0, 0, 0);

  EXPECT_EQ(ret, SQL_NO_DATA);

  ret = SQLError(0, 0, this->stmt, 0, 0, 0, 0, 0);

  EXPECT_EQ(ret, SQL_NO_DATA);

  ret = SQLError(0, 0, 0, 0, 0, 0, 0, 0);

  EXPECT_EQ(ret, SQL_INVALID_HANDLE);

  this->disconnect();
}

// -AL- todo:
// add an [ ] 1) env error -> wrong env attribute? - see TestSQLSetEnvAttrODBCVersionInvalid
// [ ] 2) conn error -> set unsettable conn attr SQLSetConnectAttr? - see TestSQLSetConnectAttrAsyncDbcEventUnsupported
// [x] 3) stmt error.

TYPED_TEST(FlightSQLODBCTestBase, TestSQLErrorStmtError) {
  // Test ODBC 2.0 API SQLError.
  // Known Windows Driver Manager (DM) behavior:
  // When application passes buffer length greater than SQL_MAX_MESSAGE_LENGTH (512),
  // DM passes 512 as buffer length to SQLError.
  this->connect();

  std::wstring wsql = L"1";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));

  EXPECT_EQ(ret, SQL_ERROR);

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ret = SQLError(0, 0, this->stmt, sql_state, &native_error, message,
                 SQL_MAX_MESSAGE_LENGTH, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(message_length, 70);

  EXPECT_EQ(native_error, 100);

  EXPECT_EQ(std::wstring(sql_state), std::wstring(L"HY000"));

  EXPECT_TRUE(!std::wstring(message).empty());

  this->disconnect();
}

}  // namespace arrow::flight::sql::odbc
