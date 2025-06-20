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

// Driver Information

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ACTIVE_ENVIRONMENTS) {
  this->connect();

  validate(conn, SQL_ACTIVE_ENVIRONMENTS, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ASYNC_DBC_FUNCTIONS) {
  this->connect();

#ifdef SQL_ASYNC_DBC_FUNCTIONS
  validate(conn, SQL_ASYNC_DBC_FUNCTIONS,
           static_cast<SQLUINTEGER>(SQL_ASYNC_DBC_NOT_CAPABLE));
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ASYNC_MODE) {
  this->connect();

  validate(conn, SQL_ASYNC_MODE, static_cast<SQLUINTEGER>(SQL_AM_NONE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ASYNC_NOTIFICATION) {
  this->connect();

#ifdef SQL_ASYNC_NOTIFICATION
  validate(conn, SQL_ASYNC_NOTIFICATION,
           static_cast<SQLUINTEGER>(SQL_ASYNC_NOTIFICATION_NOT_CAPABLE));
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_BATCH_ROW_COUNT) {
  this->connect();

  validate(conn, SQL_BATCH_ROW_COUNT, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_BATCH_SUPPORT) {
  this->connect();

  validate(conn, SQL_BATCH_SUPPORT, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DATA_SOURCE_NAME) {
  this->connect();

  validate(conn, SQL_DATA_SOURCE_NAME, L"");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DRIVER_AWARE_POOLING_SUPPORTED) {
  // A driver does not need to implement SQL_DRIVER_AWARE_POOLING_SUPPORTED and the
  // Driver Manager will not honor to the driver's return value.
  this->connect();

  validate(conn, SQL_DRIVER_AWARE_POOLING_SUPPORTED,
           static_cast<SQLUINTEGER>(SQL_DRIVER_AWARE_POOLING_NOT_CAPABLE));

  this->disconnect();
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DRIVER_HDBC) {
  this->connect();

  // Value returned from driver manager is the connection address
  validateGreaterThan(conn, SQL_DRIVER_HDBC, static_cast<SQLULEN>(0));

  this->disconnect();
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DRIVER_HDESC) {
  // TODO This is failing due to no descriptor being created
  GTEST_SKIP();
  this->connect();

  validate(conn, SQL_DRIVER_HDESC, static_cast<SQLULEN>(0));

  this->disconnect();
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DRIVER_HENV) {
  this->connect();

  // Value returned from driver manager is the env address
  validateGreaterThan(conn, SQL_DRIVER_HENV, static_cast<SQLULEN>(0));

  this->disconnect();
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DRIVER_HLIB) {
  this->connect();

  // An SQLULEN value, the hinst from the load library returned to the Driver Manager when
  // it loaded the driver DLL on a Microsoft Windows operating system, or its equivalent
  // on another operating system.
  validateGreaterThan(conn, SQL_DRIVER_HLIB, static_cast<SQLULEN>(0));

  this->disconnect();
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DRIVER_HSTMT) {
  // TODO This is failing due to no statement being created
  // This should run after SQLGetStmtAttr is implemented
  GTEST_SKIP();
  this->connect();

  validate(conn, SQL_DRIVER_HSTMT, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DRIVER_NAME) {
  this->connect();

  validate(conn, SQL_DRIVER_NAME, L"Arrow Flight ODBC Driver");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DRIVER_ODBC_VER) {
  this->connect();

  validate(conn, SQL_DRIVER_ODBC_VER, L"03.80");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DRIVER_VER) {
  this->connect();

  validate(conn, SQL_DRIVER_VER, L"00.09.0000.0");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DYNAMIC_CURSOR_ATTRIBUTES1) {
  this->connect();

  validate(conn, SQL_DYNAMIC_CURSOR_ATTRIBUTES1, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DYNAMIC_CURSOR_ATTRIBUTES2) {
  this->connect();

  validate(conn, SQL_DYNAMIC_CURSOR_ATTRIBUTES2, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1) {
  this->connect();

  validate(conn, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1,
           static_cast<SQLUINTEGER>(SQL_CA1_NEXT));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2) {
  this->connect();

  validate(conn, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2,
           static_cast<SQLUINTEGER>(SQL_CA2_READ_ONLY_CONCURRENCY));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_FILE_USAGE) {
  this->connect();

  validate(conn, SQL_FILE_USAGE, static_cast<SQLUSMALLINT>(SQL_FILE_NOT_SUPPORTED));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_GETDATA_EXTENSIONS) {
  this->connect();

  validate(conn, SQL_GETDATA_EXTENSIONS, static_cast<SQLUINTEGER>(3));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_INFO_SCHEMA_VIEWS) {
  this->connect();

  // An SQLUINTEGER bitmask enumerating the views in the INFORMATION_SCHEMA that are
  // supported by the driver.
  validateGreaterThan(conn, SQL_INFO_SCHEMA_VIEWS, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_KEYSET_CURSOR_ATTRIBUTES1) {
  this->connect();

  validate(conn, SQL_KEYSET_CURSOR_ATTRIBUTES1, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_KEYSET_CURSOR_ATTRIBUTES2) {
  this->connect();

  validate(conn, SQL_KEYSET_CURSOR_ATTRIBUTES2, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MAX_ASYNC_CONCURRENT_STATEMENTS) {
  this->connect();

  validate(conn, SQL_MAX_ASYNC_CONCURRENT_STATEMENTS, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MAX_CONCURRENT_ACTIVITIES) {
  // Driver manager returns failure code
  GTEST_SKIP();
  this->connect();

  validate(conn, SQL_MAX_CONCURRENT_ACTIVITIES, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MAX_DRIVER_CONNECTIONS) {
  this->connect();

  validate(conn, SQL_MAX_DRIVER_CONNECTIONS, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ODBC_INTERFACE_CONFORMANCE) {
  this->connect();

  validate(conn, SQL_ODBC_INTERFACE_CONFORMANCE, static_cast<SQLUINTEGER>(SQL_OIC_CORE));

  this->disconnect();
}

// case SQL_ODBC_STANDARD_CLI_CONFORMANCE: - mentioned in SQLGetInfo spec with no
// description and there is no constant for this.
TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ODBC_STANDARD_CLI_CONFORMANCE) {
  // Type not supported in odbc_connection.cc
  GTEST_SKIP();
  this->connect();

  // Type does not exist in sql.h
  // validate(conn, SQL_ODBC_STANDARD_CLI_CONFORMANCE, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ODBC_VER) {
  // This is implemented only in the Driver Manager.
  this->connect();

  validate(conn, SQL_ODBC_VER, L"03.80.0000");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_PARAM_ARRAY_ROW_COUNTS) {
  this->connect();

  validate(conn, SQL_PARAM_ARRAY_ROW_COUNTS, static_cast<SQLUINTEGER>(SQL_PARC_NO_BATCH));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_PARAM_ARRAY_SELECTS) {
  this->connect();

  validate(conn, SQL_PARAM_ARRAY_SELECTS, static_cast<SQLUINTEGER>(SQL_PAS_NO_SELECT));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ROW_UPDATES) {
  this->connect();

  validate(conn, SQL_ROW_UPDATES, L"N");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_SEARCH_PATTERN_ESCAPE) {
  this->connect();

  validate(conn, SQL_SEARCH_PATTERN_ESCAPE, L"\\");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_SERVER_NAME) {
  this->connect();

  validateNotEmptySQLWCHAR(conn, SQL_SERVER_NAME);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_STATIC_CURSOR_ATTRIBUTES1) {
  this->connect();

  validate(conn, SQL_STATIC_CURSOR_ATTRIBUTES1, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_STATIC_CURSOR_ATTRIBUTES2) {
  this->connect();

  validate(conn, SQL_STATIC_CURSOR_ATTRIBUTES2, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

// wchar
void validateNotEmptySQLWCHAR(SQLHDBC connection, SQLUSMALLINT infoType) {
  SQLWCHAR info_value[ODBC_BUFFER_SIZE] = L"";
  SQLSMALLINT message_length;

  SQLRETURN ret =
      SQLGetInfo(connection, infoType, info_value, ODBC_BUFFER_SIZE, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(wcslen(info_value), 0);
}

// wchar
void validate(SQLHDBC connection, SQLUSMALLINT infoType, SQLWCHAR* expected_value) {
  SQLWCHAR info_value[ODBC_BUFFER_SIZE] = L"";
  SQLSMALLINT message_length;

  SQLRETURN ret =
      SQLGetInfo(connection, infoType, info_value, ODBC_BUFFER_SIZE, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(*info_value, *expected_value);
}

// unsigned short
void validate(SQLHDBC connection, SQLUSMALLINT infoType, SQLUSMALLINT expected_value) {
  SQLUSMALLINT info_value;
  SQLSMALLINT message_length;

  SQLRETURN ret = SQLGetInfo(connection, infoType, &info_value, 0, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(info_value, expected_value);
}

// unsigned long
void validate(SQLHDBC connection, SQLUSMALLINT infoType, SQLUINTEGER expected_value) {
  SQLUINTEGER info_value;
  SQLSMALLINT message_length;

  SQLRETURN ret = SQLGetInfo(connection, infoType, &info_value, 0, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(info_value, expected_value);
}

// unsigned long
void validateGreaterThan(SQLHDBC connection, SQLUSMALLINT infoType,
                         SQLUINTEGER compared_value) {
  SQLUINTEGER info_value;
  SQLSMALLINT message_length;

  SQLRETURN ret = SQLGetInfo(connection, infoType, &info_value, 0, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(info_value, compared_value);
}

// sql unsigned length
void validate(SQLHDBC connection, SQLUSMALLINT infoType, SQLULEN expected_value) {
  SQLULEN info_value;
  SQLSMALLINT message_length;

  SQLRETURN ret = SQLGetInfo(connection, infoType, &info_value, 0, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(info_value, expected_value);
}

// sql unsigned length
void validateGreaterThan(SQLHDBC connection, SQLUSMALLINT infoType,
                         SQLULEN compared_value) {
  SQLULEN info_value;
  SQLSMALLINT message_length;

  SQLRETURN ret = SQLGetInfo(connection, infoType, &info_value, 0, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(info_value, compared_value);
}

}  // namespace integration_tests
}  // namespace odbc
}  // namespace flight
}  // namespace arrow
