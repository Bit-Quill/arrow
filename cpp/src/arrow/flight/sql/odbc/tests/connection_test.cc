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

TEST(SQLDriverConnect, TestSQLDriverConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Connect string
  ASSERT_OK_AND_ASSIGN(std::string connect_str,
                       arrow::internal::GetEnvVar(TEST_CONNECT_STR));
  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR outstr[ODBC_BUFFER_SIZE] = L"";
  SQLSMALLINT outstrlen;

  // Connecting to ODBC server.
  ret = SQLDriverConnect(conn, NULL, &connect_str0[0],
                         static_cast<SQLSMALLINT>(connect_str0.size()), outstr,
                         ODBC_BUFFER_SIZE, &outstrlen, SQL_DRIVER_NOPROMPT);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Check that outstr has same content as connect_str
  std::string out_connection_string = ODBC::SqlWcharToString(outstr, outstrlen);
  Connection::ConnPropertyMap out_properties;
  Connection::ConnPropertyMap in_properties;
  ODBC::ODBCConnection::getPropertiesFromConnString(out_connection_string,
                                                    out_properties);
  ODBC::ODBCConnection::getPropertiesFromConnString(connect_str, in_properties);
  EXPECT_TRUE(compareConnPropertyMap(out_properties, in_properties));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_TRUE(ret == SQL_SUCCESS);
}

TEST(SQLGetDiagField, TestSQLGetDiagFieldForConnectFailure) {
  GTEST_SKIP();
  //  ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Connect string
  ASSERT_OK_AND_ASSIGN(std::string connect_str,
                       arrow::internal::GetEnvVar(TEST_CONNECT_STR));
  // Append invalid uid to connection string
  connect_str += std::string("uid=non_existent_id;");

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

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  SQLWCHAR diagInfoPtr[ODBC_BUFFER_SIZE];
  SQLSMALLINT string_length;

  // SQLGetDiagField(SQLSMALLINT handleType, SQLHANDLE handle, SQLSMALLINT recNumber,
  //                  SQLSMALLINT diagIdentifier, SQLPOINTER diagInfoPtr,
  //                  SQLSMALLINT bufferLength, SQLSMALLINT * stringLengthPtr)

  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, 0, SQL_DIAG_NUMBER, diagInfoPtr,
                         ODBC_BUFFER_SIZE, &string_length);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, 0, SQL_DIAG_SERVER_NAME, diagInfoPtr,
                         ODBC_BUFFER_SIZE, &string_length);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, 1, SQL_DIAG_MESSAGE_TEXT, diagInfoPtr,
                         ODBC_BUFFER_SIZE, &string_length);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // SQL_DIAG_NATIVE
  // SQL_DIAG_SQLSTATE

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // SQL_NO_DATA = 100
  EXPECT_TRUE(ret == SQL_NO_DATA);

  EXPECT_TRUE(string_length > 200);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_TRUE(ret == SQL_SUCCESS);
}

TEST(SQLSetEnvAttr, TestSQLGetDiagRecForVersionUpdateFailure) {
  GTEST_SKIP();
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_TRUE(return_env == SQL_SUCCESS);

  // Attempt to set to unsupported version
  SQLRETURN return_set =
      SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(1), 0);

  EXPECT_TRUE(return_set == SQL_ERROR);

  SQLWCHAR sql_state[6];
  SQLINTEGER native_error;
  SQLWCHAR message[ODBC_BUFFER_SIZE];
  SQLSMALLINT message_length;

  SQLRETURN diag_ret = SQLGetDiagRec(SQL_HANDLE_ENV, env, 1, sql_state, &native_error,
                                      message, ODBC_BUFFER_SIZE, &message_length);

  EXPECT_TRUE(diag_ret == SQL_SUCCESS);
  // EXPECT_TRUE(ret == SQL_NO_DATA);

  EXPECT_TRUE(message_length > 50);

  EXPECT_TRUE(sql_state[0] == 'H');

  EXPECT_TRUE(native_error == 200);
}

TEST(SQLGetDiagRec, TestSQLGetDiagRecForConnectFailure) {
  // GTEST_SKIP();
  //  ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Connect string
  ASSERT_OK_AND_ASSIGN(std::string connect_str,
                       arrow::internal::GetEnvVar(TEST_CONNECT_STR));
  // Append invalid uid to connection string
  connect_str += std::string("uid=non_existent_id;");

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

    if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  SQLWCHAR sql_state[6];
  SQLINTEGER native_error;
  SQLWCHAR message[ODBC_BUFFER_SIZE];
  SQLSMALLINT message_length;

  // SQLWCHAR* sql_state_ptr = &sql_state[0];
  // SQLINTEGER* native_error_ptr = &native_error;
  // SQLWCHAR* message_ptr = &message[0];
  // SQLSMALLINT* message_length_ptr = &message_length;

  ret = SQLGetDiagRec(SQL_HANDLE_DBC, conn, 1, sql_state, &native_error, message,
                       ODBC_BUFFER_SIZE, &message_length);
  // ret = SQLGetDiagRec(SQL_HANDLE_DBC, conn, 1, sql_state_ptr, native_error_ptr,
  // message_ptr, ODBC_BUFFER_SIZE, message_length_ptr); ret =
  // SQLGetDiagRec(SQL_HANDLE_DBC, conn, 1, &sql_state[0], &native_error, &message[0],
  // ODBC_BUFFER_SIZE, &message_length);

  EXPECT_TRUE(ret == SQL_SUCCESS);
  // EXPECT_TRUE(ret == SQL_NO_DATA);

  EXPECT_TRUE(message_length > 200);

  EXPECT_TRUE(native_error == 200);

  // HY000
  EXPECT_TRUE(sql_state[0] == 'S');
  EXPECT_TRUE(sql_state[1] == '1');
  EXPECT_TRUE(sql_state[2] == '0');
  EXPECT_TRUE(sql_state[3] == '0');
  EXPECT_TRUE(sql_state[4] == '0');

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_TRUE(ret == SQL_SUCCESS);
}

}  // namespace integration_tests
}  // namespace odbc
}  // namespace flight
}  // namespace arrow

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
