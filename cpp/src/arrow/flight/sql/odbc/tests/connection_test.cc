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

TEST(SQLAllocHandle, TestSQLAllocHandleEnv) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

  EXPECT_TRUE(env != NULL);
}

TEST(SQLAllocEnv, TestSQLAllocEnv) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_value = SQLAllocEnv(&env);

  EXPECT_TRUE(return_value == SQL_SUCCESS);
}

TEST(SQLAllocHandle, TestSQLAllocHandleConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN return_value = SQLAllocEnv(&env);

  EXPECT_TRUE(return_value == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  SQLRETURN return_alloc_handle = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(return_alloc_handle == SQL_SUCCESS);
}

TEST(SQLAllocConnect, TestSQLAllocHandleConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN return_value = SQLAllocEnv(&env);

  EXPECT_TRUE(return_value == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  SQLRETURN return_alloc_connect = SQLAllocConnect(env, &conn);

  EXPECT_TRUE(return_alloc_connect == SQL_SUCCESS);
}

TEST(SQLFreeHandle, TestSQLFreeHandleEnv) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

  // Free an environment handle
  SQLRETURN return_value = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_TRUE(return_value == SQL_SUCCESS);
}

TEST(SQLFreeEnv, TestSQLFreeEnv) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

  // Free an environment handle
  SQLRETURN return_value = SQLFreeEnv(env);

  EXPECT_TRUE(return_value == SQL_SUCCESS);
}

TEST(SQLFreeHandle, TestSQLFreeHandleConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN return_value = SQLAllocEnv(&env);

  EXPECT_TRUE(return_value == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  SQLRETURN return_alloc_handle = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(return_alloc_handle == SQL_SUCCESS);

  // Free the created connection using free handle
  SQLRETURN return_free_handle = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_TRUE(return_free_handle == SQL_SUCCESS);
}

TEST(SQLFreeConnect, TestSQLFreeConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_TRUE(return_env == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  SQLRETURN return_alloc_handle = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(return_alloc_handle == SQL_SUCCESS);

  // Free the created connection using free connect
  SQLRETURN return_free_connect = SQLFreeConnect(conn);

  EXPECT_TRUE(return_free_connect == SQL_SUCCESS);
}

TEST(SQLGetEnvAttr, TestSQLGetEnvAttrODBCVersion) {
  // ODBC Environment
  SQLHENV env;

  SQLINTEGER version;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_TRUE(return_env == SQL_SUCCESS);

  SQLRETURN return_get = SQLGetEnvAttr(env, SQL_ATTR_ODBC_VERSION, &version, 0, 0);

  EXPECT_TRUE(return_get == SQL_SUCCESS);

  EXPECT_EQ(version, SQL_OV_ODBC2);
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrODBCVersionValid) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_TRUE(return_env == SQL_SUCCESS);

  // Attempt to set to unsupported version
  SQLRETURN return_set =
      SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC2), 0);

  EXPECT_TRUE(return_set == SQL_SUCCESS);
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrODBCVersionInvalid) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_TRUE(return_env == SQL_SUCCESS);

  // Attempt to set to unsupported version
  SQLRETURN return_set =
      SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(1), 0);

  EXPECT_TRUE(return_set == SQL_ERROR);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetEnvAttrOutputNTS) {
  this->connect();

  SQLINTEGER output_nts;

  SQLRETURN return_get = SQLGetEnvAttr(this->env, SQL_ATTR_OUTPUT_NTS, &output_nts, 0, 0);

  EXPECT_TRUE(return_get == SQL_SUCCESS);

  EXPECT_EQ(output_nts, SQL_TRUE);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetEnvAttrGetLength) {
  // Test is disabled because call to SQLGetEnvAttr is handled by the driver manager on
  // Windows. This test case can be potentially used on macOS/Linux
  GTEST_SKIP();

  this->connect();

  SQLINTEGER length;

  SQLRETURN return_get =
      SQLGetEnvAttr(this->env, SQL_ATTR_ODBC_VERSION, nullptr, 0, &length);

  EXPECT_TRUE(return_get == SQL_SUCCESS);

  EXPECT_EQ(length, sizeof(SQLINTEGER));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetEnvAttrNullValuePointer) {
  // Test is disabled because call to SQLGetEnvAttr is handled by the driver manager on
  // Windows. This test case can be potentially used on macOS/Linux
  GTEST_SKIP();
  this->connect();

  SQLRETURN return_get =
      SQLGetEnvAttr(this->env, SQL_ATTR_ODBC_VERSION, nullptr, 0, nullptr);

  EXPECT_TRUE(return_get == SQL_ERROR);

  this->disconnect();
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrOutputNTSValid) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_TRUE(return_env == SQL_SUCCESS);

  // Attempt to set to output nts to supported version
  SQLRETURN return_set =
      SQLSetEnvAttr(env, SQL_ATTR_OUTPUT_NTS, reinterpret_cast<void*>(SQL_TRUE), 0);

  EXPECT_TRUE(return_set == SQL_SUCCESS);
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrOutputNTSInvalid) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_TRUE(return_env == SQL_SUCCESS);

  // Attempt to set to output nts to unsupported false
  SQLRETURN return_set =
      SQLSetEnvAttr(env, SQL_ATTR_OUTPUT_NTS, reinterpret_cast<void*>(SQL_FALSE), 0);

  EXPECT_TRUE(return_set == SQL_ERROR);
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrNullValuePointer) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_TRUE(return_env == SQL_SUCCESS);

  // Attempt to set using bad data pointer
  SQLRETURN return_set = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, nullptr, 0);

  EXPECT_TRUE(return_set == SQL_ERROR);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLDriverConnect) {
  // ODBC Environment
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

  // Connect string
  std::string connect_str = this->getConnectionString();
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

  EXPECT_EQ(ret, SQL_SUCCESS);

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

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLDriverConnectInvalidUid) {
  // ODBC Environment
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
  std::string connect_str = getInvalidConnectionString();

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

  VerifyOdbcErrorState(SQL_HANDLE_DBC, conn, error_state_28000);

  std::string out_connection_string = ODBC::SqlWcharToString(outstr, outstrlen);
  EXPECT_TRUE(out_connection_string.empty());

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLConnect) {
  // ODBC Environment
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

  // Connect string
  std::string connect_str = this->getConnectionString();

  // Write connection string content into a DSN,
  // must succeed before continuing
  std::string uid(""), pwd("");
  ASSERT_TRUE(writeDSN(connect_str));

  std::string dsn(TEST_DSN);
  ASSERT_OK_AND_ASSIGN(std::wstring wdsn, arrow::util::UTF8ToWideString(dsn));
  ASSERT_OK_AND_ASSIGN(std::wstring wuid, arrow::util::UTF8ToWideString(uid));
  ASSERT_OK_AND_ASSIGN(std::wstring wpwd, arrow::util::UTF8ToWideString(pwd));
  std::vector<SQLWCHAR> dsn0(wdsn.begin(), wdsn.end());
  std::vector<SQLWCHAR> uid0(wuid.begin(), wuid.end());
  std::vector<SQLWCHAR> pwd0(wpwd.begin(), wpwd.end());

  // Connecting to ODBC server. Empty uid and pwd should be ignored.
  ret = SQLConnect(conn, dsn0.data(), static_cast<SQLSMALLINT>(dsn0.size()), uid0.data(),
                   static_cast<SQLSMALLINT>(uid0.size()), pwd0.data(),
                   static_cast<SQLSMALLINT>(pwd0.size()));

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Remove DSN
  EXPECT_TRUE(UnregisterDsn(wdsn));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLConnectInputUidPwd) {
  // ODBC Environment
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

  // Connect string
  std::string connect_str = getConnectionString();

  // Retrieve valid uid and pwd, assumes TEST_CONNECT_STR contains uid and pwd
  Connection::ConnPropertyMap properties;
  ODBC::ODBCConnection::getPropertiesFromConnString(connect_str, properties);
  std::string uid_key("uid");
  std::string pwd_key("pwd");
  std::string uid = properties[uid_key];
  std::string pwd = properties[pwd_key];

  // Write connection string content without uid and pwd into a DSN,
  // must succeed before continuing
  properties.erase(uid_key);
  properties.erase(pwd_key);
  ASSERT_TRUE(writeDSN(properties));

  std::string dsn(TEST_DSN);
  ASSERT_OK_AND_ASSIGN(std::wstring wdsn, arrow::util::UTF8ToWideString(dsn));
  ASSERT_OK_AND_ASSIGN(std::wstring wuid, arrow::util::UTF8ToWideString(uid));
  ASSERT_OK_AND_ASSIGN(std::wstring wpwd, arrow::util::UTF8ToWideString(pwd));
  std::vector<SQLWCHAR> dsn0(wdsn.begin(), wdsn.end());
  std::vector<SQLWCHAR> uid0(wuid.begin(), wuid.end());
  std::vector<SQLWCHAR> pwd0(wpwd.begin(), wpwd.end());

  // Connecting to ODBC server.
  ret = SQLConnect(conn, dsn0.data(), static_cast<SQLSMALLINT>(dsn0.size()), uid0.data(),
                   static_cast<SQLSMALLINT>(uid0.size()), pwd0.data(),
                   static_cast<SQLSMALLINT>(pwd0.size()));

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Remove DSN
  EXPECT_TRUE(UnregisterDsn(wdsn));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLConnectInvalidUid) {
  // ODBC Environment
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

  // Connect string
  std::string connect_str = getConnectionString();

  // Retrieve valid uid and pwd, assumes TEST_CONNECT_STR contains uid and pwd
  Connection::ConnPropertyMap properties;
  ODBC::ODBCConnection::getPropertiesFromConnString(connect_str, properties);
  std::string uid = properties[std::string("uid")];
  std::string pwd = properties[std::string("pwd")];

  // Append invalid uid to connection string
  connect_str += std::string("uid=non_existent_id;");

  // Write connection string content into a DSN,
  // must succeed before continuing
  ASSERT_TRUE(writeDSN(connect_str));

  std::string dsn(TEST_DSN);
  ASSERT_OK_AND_ASSIGN(std::wstring wdsn, arrow::util::UTF8ToWideString(dsn));
  ASSERT_OK_AND_ASSIGN(std::wstring wuid, arrow::util::UTF8ToWideString(uid));
  ASSERT_OK_AND_ASSIGN(std::wstring wpwd, arrow::util::UTF8ToWideString(pwd));
  std::vector<SQLWCHAR> dsn0(wdsn.begin(), wdsn.end());
  std::vector<SQLWCHAR> uid0(wuid.begin(), wuid.end());
  std::vector<SQLWCHAR> pwd0(wpwd.begin(), wpwd.end());

  // Connecting to ODBC server.
  ret = SQLConnect(conn, dsn0.data(), static_cast<SQLSMALLINT>(dsn0.size()), uid0.data(),
                   static_cast<SQLSMALLINT>(uid0.size()), pwd0.data(),
                   static_cast<SQLSMALLINT>(pwd0.size()));

  // UID specified in DSN will take precedence,
  // so connection still fails despite passing valid uid in SQLConnect call
  EXPECT_TRUE(ret == SQL_ERROR);

  VerifyOdbcErrorState(SQL_HANDLE_DBC, conn, error_state_28000);

  // Remove DSN
  EXPECT_TRUE(UnregisterDsn(wdsn));

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLConnectDSNPrecedence) {
  // ODBC Environment
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

  // Connect string
  std::string connect_str = getConnectionString();

  // Write connection string content into a DSN,
  // must succeed before continuing

  // Pass incorrect uid and password to SQLConnect, they will be ignored.
  // Assumes TEST_CONNECT_STR contains uid and pwd
  std::string uid("non_existent_id"), pwd("non_existent_password");
  ASSERT_TRUE(writeDSN(connect_str));

  std::string dsn(TEST_DSN);
  ASSERT_OK_AND_ASSIGN(std::wstring wdsn, arrow::util::UTF8ToWideString(dsn));
  ASSERT_OK_AND_ASSIGN(std::wstring wuid, arrow::util::UTF8ToWideString(uid));
  ASSERT_OK_AND_ASSIGN(std::wstring wpwd, arrow::util::UTF8ToWideString(pwd));
  std::vector<SQLWCHAR> dsn0(wdsn.begin(), wdsn.end());
  std::vector<SQLWCHAR> uid0(wuid.begin(), wuid.end());
  std::vector<SQLWCHAR> pwd0(wpwd.begin(), wpwd.end());

  // Connecting to ODBC server.
  ret = SQLConnect(conn, dsn0.data(), static_cast<SQLSMALLINT>(dsn0.size()), uid0.data(),
                   static_cast<SQLSMALLINT>(uid0.size()), pwd0.data(),
                   static_cast<SQLSMALLINT>(pwd0.size()));

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Remove DSN
  EXPECT_TRUE(UnregisterDsn(wdsn));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TEST(SQLDisconnect, TestSQLDisconnectWithoutConnection) {
  // ODBC Environment
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

  // Attempt to disconnect without a connection, expect to fail
  ret = SQLDisconnect(conn);

  EXPECT_TRUE(ret == SQL_ERROR);

  // Expect ODBC driver manager to return error state
  VerifyOdbcErrorState(SQL_HANDLE_DBC, conn, error_state_08003);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

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

  // 28000
  EXPECT_EQ(sql_state[0], '2');
  EXPECT_EQ(sql_state[1], '8');
  EXPECT_EQ(sql_state[2], '0');
  EXPECT_EQ(sql_state[3], '0');
  EXPECT_EQ(sql_state[4], '0');

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

  // 28000
  EXPECT_EQ(sql_state[0], '2');
  EXPECT_EQ(sql_state[1], '8');
  EXPECT_EQ(sql_state[2], '0');
  EXPECT_EQ(sql_state[3], '0');
  EXPECT_EQ(sql_state[4], '0');

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TYPED_TEST(FlightSQLODBCTestBase, TestConnect) {
  // Verifies connect and disconnect works on its own
  this->connect();
  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLAllocFreeStmt) {
  this->connect();
  SQLHSTMT statement;

  // Allocate a statement using alloc statement
  SQLRETURN ret = SQLAllocStmt(this->conn, &statement);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // TODO Uncomment once SQLExecDirect is implemented
  // SQLWCHAR sql_buffer[ODBC_BUFFER_SIZE] = L"SELECT 1";
  // ret = SQLExecDirect(statement, sql_buffer, SQL_NTS);

  // EXPECT_EQ(ret, SQL_SUCCESS);

  // ret = SQLFreeStmt(statement, SQL_CLOSE);

  // EXPECT_EQ(ret, SQL_SUCCESS);

  // Free statement handle
  ret = SQLFreeStmt(statement, SQL_DROP);

  EXPECT_EQ(ret, SQL_SUCCESS);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestCloseConnectionWithOpenStatement) {
  // Test is disabled as disconnecting without closing statement fails on Windows.
  // This test case can be potentially used on macOS/Linux.
  GTEST_SKIP();
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;
  SQLHSTMT statement;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Connect string
  std::string connect_str = this->getConnectionString();
  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR outstr[ODBC_BUFFER_SIZE] = L"";
  SQLSMALLINT outstrlen;

  // Connecting to ODBC server.
  ret = SQLDriverConnect(conn, NULL, &connect_str0[0],
                         static_cast<SQLSMALLINT>(connect_str0.size()), outstr,
                         ODBC_BUFFER_SIZE, &outstrlen, SQL_DRIVER_NOPROMPT);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Allocate a statement using alloc statement
  ret = SQLAllocStmt(conn, &statement);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Disconnect from ODBC without closing the statement first
  ret = SQLDisconnect(conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

// -AL- see if moving ConnectAttr tests to connection_test resolves the seg fault issue
// Tests are copied without changes
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrAsyncDbcEventUnsupported) {
  this->connect();

#ifdef SQL_ATTR_ASYNC_DBC_EVENT
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_EVENT, 0, 0);

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
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_PCALLBACK, 0, 0);

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

  SQLRETURN ret =
      SQLSetConnectAttr(this->conn, SQL_ATTR_TXN_ISOLATION,
                        reinterpret_cast<SQLPOINTER>(SQL_TXN_READ_UNCOMMITTED), 0);
  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrDbcInfoTokenSetOnly) {
  this->connect();

#ifdef SQL_ATTR_DBC_INFO_TOKEN
  // Verify that set-only attribute cannot be read
  SQLPOINTER ptr = NULL;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_DBC_INFO_TOKEN, ptr, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY092);
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrOdbcCursorsDMOnly) {
  this->connect();

  // Verify that DM-only attribute is handled by driver manager
  SQLULEN cursor_attr;
  SQLRETURN ret =
      SQLGetConnectAttr(this->conn, SQL_ATTR_ODBC_CURSORS, &cursor_attr, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(cursor_attr, SQL_CUR_USE_DRIVER);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrTraceDMOnly) {
  this->connect();

  // Verify that DM-only attribute is handled by driver manager
  SQLUINTEGER trace;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_TRACE, &trace, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(trace, SQL_OPT_TRACE_OFF);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrTraceFileDMOnly) {
  this->connect();

  // Verify that DM-only attribute is handled by driver manager
  SQLWCHAR outstr[ODBC_BUFFER_SIZE];
  SQLINTEGER outstrlen;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_TRACEFILE, outstr,
                                    ODBC_BUFFER_SIZE, &outstrlen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Length is returned in bytes for SQLGetConnectAttr,
  // we want the number of characters
  outstrlen /= driver::odbcabstraction::GetSqlWCharSize();
  std::string out_connection_string =
      ODBC::SqlWcharToString(outstr, static_cast<SQLSMALLINT>(outstrlen));
  EXPECT_TRUE(!out_connection_string.empty());

  out_connection_string =
      ODBC::SqlWcharToString(outstr, static_cast<SQLSMALLINT>(outstrlen));
  EXPECT_TRUE(!out_connection_string.empty());

  out_connection_string =
      ODBC::SqlWcharToString(outstr, static_cast<SQLSMALLINT>(outstrlen));
  EXPECT_TRUE(!out_connection_string.empty());

  out_connection_string =
      ODBC::SqlWcharToString(outstr, static_cast<SQLSMALLINT>(outstrlen));
  EXPECT_TRUE(!out_connection_string.empty());

  out_connection_string =
      ODBC::SqlWcharToString(outstr, static_cast<SQLSMALLINT>(outstrlen));
  EXPECT_TRUE(!out_connection_string.empty());

  out_connection_string =
      ODBC::SqlWcharToString(outstr, static_cast<SQLSMALLINT>(outstrlen));
  EXPECT_TRUE(!out_connection_string.empty());

  out_connection_string =
      ODBC::SqlWcharToString(outstr, static_cast<SQLSMALLINT>(outstrlen));
  EXPECT_TRUE(!out_connection_string.empty());

  out_connection_string =
      ODBC::SqlWcharToString(outstr, static_cast<SQLSMALLINT>(outstrlen));
  EXPECT_TRUE(!out_connection_string.empty());

  out_connection_string =
      ODBC::SqlWcharToString(outstr, static_cast<SQLSMALLINT>(outstrlen));
  EXPECT_TRUE(!out_connection_string.empty());

  out_connection_string =
      ODBC::SqlWcharToString(outstr, static_cast<SQLSMALLINT>(outstrlen));
  EXPECT_TRUE(!out_connection_string.empty());

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrTranslateLibUnsupported) {
  this->connect();

  SQLWCHAR outstr[ODBC_BUFFER_SIZE];
  SQLINTEGER outstrlen;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_TRANSLATE_LIB, outstr,
                                    ODBC_BUFFER_SIZE, &outstrlen);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrTranslateOptionUnsupported) {
  this->connect();

  SQLINTEGER option;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_TRANSLATE_OPTION, &option, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrTxnIsolationUnsupported) {
  this->connect();

  SQLINTEGER isolation;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_TXN_ISOLATION, &isolation, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase,
           TestSQLGetConnectAttrAsyncDbcFunctionsEnableUnsupported) {
  this->connect();

#ifdef SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE
  // Verifies that the Windows driver manager returns HY114 for unsupported functionality
  SQLUINTEGER enable;
  SQLRETURN ret =
      SQLGetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE, &enable, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY114);
#endif

  this->disconnect();
}

// Tests for supported attributes

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrAsyncDbcEventDefault) {
  this->connect();

#ifdef SQL_ATTR_ASYNC_DBC_EVENT
  SQLPOINTER ptr = NULL;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_EVENT, ptr, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ptr, reinterpret_cast<SQLPOINTER>(NULL));
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrAsyncDbcPcallbackDefault) {
  this->connect();

#ifdef SQL_ATTR_ASYNC_DBC_PCALLBACK
  SQLPOINTER ptr = NULL;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_PCALLBACK, ptr, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ptr, reinterpret_cast<SQLPOINTER>(NULL));
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrAsyncDbcPcontextDefault) {
  this->connect();

#ifdef SQL_ATTR_ASYNC_DBC_PCONTEXT
  SQLPOINTER ptr = NULL;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_PCONTEXT, ptr, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ptr, reinterpret_cast<SQLPOINTER>(NULL));
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrAsyncEnableDefault) {
  this->connect();

  SQLULEN enable;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ASYNC_ENABLE, &enable, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(enable, SQL_ASYNC_ENABLE_OFF);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrAutoIpdDefault) {
  this->connect();

  SQLUINTEGER ipd;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_AUTO_IPD, &ipd, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ipd, static_cast<SQLUINTEGER>(SQL_FALSE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrAutocommitDefault) {
  this->connect();

  SQLUINTEGER auto_commit;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_AUTOCOMMIT, &auto_commit, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(auto_commit, SQL_AUTOCOMMIT_ON);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrEnlistInDtcDefault) {
  this->connect();

  SQLPOINTER ptr = NULL;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ENLIST_IN_DTC, ptr, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ptr, reinterpret_cast<SQLPOINTER>(NULL));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrQuietModeDefault) {
  this->connect();

  HWND ptr = NULL;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_QUIET_MODE, ptr, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ptr, reinterpret_cast<SQLPOINTER>(NULL));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrAccessModeValid) {
  this->connect();

  // The driver always returns SQL_MODE_READ_WRITE

  // Check default value first
  SQLUINTEGER mode = -1;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ACCESS_MODE, &mode, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(mode, SQL_MODE_READ_WRITE);

  ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ACCESS_MODE,
                          reinterpret_cast<SQLPOINTER>(SQL_MODE_READ_WRITE), 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  mode = -1;

  ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ACCESS_MODE, &mode, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(mode, SQL_MODE_READ_WRITE);

  // Attempt to set to SQL_MODE_READ_ONLY, driver should return warning and not error
  ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ACCESS_MODE,
                          reinterpret_cast<SQLPOINTER>(SQL_MODE_READ_ONLY), 0);

  EXPECT_EQ(ret, SQL_SUCCESS_WITH_INFO);

  // Verify warning status
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_01S02);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrConnectionTimeoutValid) {
  this->connect();

  // Check default value first
  SQLUINTEGER timeout = -1;
  SQLRETURN ret =
      SQLGetConnectAttr(this->conn, SQL_ATTR_CONNECTION_TIMEOUT, &timeout, 0, 0);

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

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrLoginTimeoutValid) {
  this->connect();

  // Check default value first
  SQLUINTEGER timeout = -1;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_LOGIN_TIMEOUT, &timeout, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(timeout, 0);

  ret = SQLSetConnectAttr(this->conn, SQL_ATTR_LOGIN_TIMEOUT,
                          reinterpret_cast<SQLPOINTER>(42), 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  timeout = -1;

  ret = SQLGetConnectAttr(this->conn, SQL_ATTR_LOGIN_TIMEOUT, &timeout, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(timeout, 42);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrPacketSizeValid) {
  this->connect();

  // The driver always returns 0. PACKET_SIZE value is unused by the driver.

  // Check default value first
  SQLUINTEGER size = -1;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_PACKET_SIZE, &size, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(size, 0);

  ret = SQLSetConnectAttr(this->conn, SQL_ATTR_PACKET_SIZE,
                          reinterpret_cast<SQLPOINTER>(0), 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  size = -1;

  ret = SQLGetConnectAttr(this->conn, SQL_ATTR_PACKET_SIZE, &size, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(size, 0);

  // Attempt to set to non-zero value, driver should return warning and not error
  ret = SQLSetConnectAttr(this->conn, SQL_ATTR_PACKET_SIZE,
                          reinterpret_cast<SQLPOINTER>(2), 0);

  EXPECT_EQ(ret, SQL_SUCCESS_WITH_INFO);

  // Verify warning status
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_01S02);

  this->disconnect();
}
// -AL- end of copied tests

}  // namespace arrow::flight::sql::odbc

// -AL- consider removing this. Then I will need to find a different way, maybe like other arrow tests,
// by adding an enviornment
// to clean up protobuf after all tests.
//int main(int argc, char** argv) {
//  ::testing::InitGoogleTest(&argc, argv);
//  return RUN_ALL_TESTS();
//}
