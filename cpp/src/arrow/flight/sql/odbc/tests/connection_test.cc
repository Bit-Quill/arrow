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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace arrow::flight::sql::odbc {

TYPED_TEST(FlightSQLODBCTestBase, TestSQLAllocFreeStmt) {
  this->Connect();
  SQLHSTMT statement;

  // Allocate a statement using alloc statement
  SQLRETURN ret = SQLAllocStmt(this->conn, &statement);

  EXPECT_EQ(SQL_SUCCESS, ret);

  SQLWCHAR sql_buffer[ODBC_BUFFER_SIZE] = L"SELECT 1";
  ret = SQLExecDirect(statement, sql_buffer, SQL_NTS);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Close statement handle
  ret = SQLFreeStmt(statement, SQL_CLOSE);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Free statement handle
  ret = SQLFreeStmt(statement, SQL_DROP);

  EXPECT_EQ(SQL_SUCCESS, ret);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestCloseConnectionWithOpenStatement) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;
  SQLHSTMT statement;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, ret);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Connect string
  std::string connect_str = this->GetConnectionString();
  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR out_str[ODBC_BUFFER_SIZE] = L"";
  SQLSMALLINT out_str_len;

  // Connecting to ODBC server.
  ret = SQLDriverConnect(conn, NULL, &connect_str0[0],
                         static_cast<SQLSMALLINT>(connect_str0.size()), out_str,
                         ODBC_BUFFER_SIZE, &out_str_len, SQL_DRIVER_NOPROMPT);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Allocate a statement using alloc statement
  ret = SQLAllocStmt(conn, &statement);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Disconnect from ODBC without closing the statement first
  ret = SQLDisconnect(conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(SQL_SUCCESS, ret);
}

}  // namespace arrow::flight::sql::odbc
