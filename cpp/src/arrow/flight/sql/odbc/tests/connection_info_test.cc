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

#include "arrow/flight/sql/odbc/odbc_impl/platform.h"

#include <sql.h>
#include <sqltypes.h>
#include <sqlucode.h>

#include <gtest/gtest.h>

namespace arrow::flight::sql::odbc {

// Helper Functions

// Validate unsigned short SQLUSMALLINT return value
void Validate(SQLHDBC connection, SQLUSMALLINT info_type, SQLUSMALLINT expected_value) {
  SQLUSMALLINT info_value;
  SQLSMALLINT message_length;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetInfo(connection, info_type, &info_value, 0, &message_length));

  EXPECT_EQ(expected_value, info_value);
}

// Validate unsigned long SQLUINTEGER return value
void Validate(SQLHDBC connection, SQLUSMALLINT info_type, SQLUINTEGER expected_value) {
  SQLUINTEGER info_value;
  SQLSMALLINT message_length;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetInfo(connection, info_type, &info_value, 0, &message_length));

  EXPECT_EQ(expected_value, info_value);
}

// Validate unsigned length SQLULEN return value
void Validate(SQLHDBC connection, SQLUSMALLINT info_type, SQLULEN expected_value) {
  SQLULEN info_value;
  SQLSMALLINT message_length;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetInfo(connection, info_type, &info_value, 0, &message_length));

  EXPECT_EQ(expected_value, info_value);
}

// Validate wchar string SQLWCHAR return value
void Validate(SQLHDBC connection, SQLUSMALLINT info_type, SQLWCHAR* expected_value) {
  SQLWCHAR info_value[ODBC_BUFFER_SIZE] = L"";
  SQLSMALLINT message_length;

  ASSERT_EQ(SQL_SUCCESS, SQLGetInfo(connection, info_type, info_value, ODBC_BUFFER_SIZE,
                                    &message_length));

  EXPECT_EQ(*expected_value, *info_value);
}

// Validate unsigned long SQLUINTEGER return value is greater than
void ValidateGreaterThan(SQLHDBC connection, SQLUSMALLINT info_type,
                         SQLUINTEGER compared_value) {
  SQLUINTEGER info_value;
  SQLSMALLINT message_length;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetInfo(connection, info_type, &info_value, 0, &message_length));

  EXPECT_GT(info_value, compared_value);
}

// Validate unsigned length SQLULEN return value is greater than
void ValidateGreaterThan(SQLHDBC connection, SQLUSMALLINT info_type,
                         SQLULEN compared_value) {
  SQLULEN info_value;
  SQLSMALLINT message_length;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetInfo(connection, info_type, &info_value, 0, &message_length));

  EXPECT_GT(info_value, compared_value);
}

// Validate wchar string SQLWCHAR return value is not empty
void ValidateNotEmptySQLWCHAR(SQLHDBC connection, SQLUSMALLINT info_type,
                              bool allow_truncation) {
  SQLWCHAR info_value[ODBC_BUFFER_SIZE] = L"";
  SQLSMALLINT message_length;

  SQLRETURN ret =
      SQLGetInfo(connection, info_type, info_value, ODBC_BUFFER_SIZE, &message_length);
  if (allow_truncation && ret == SQL_SUCCESS_WITH_INFO) {
    ASSERT_EQ(SQL_SUCCESS_WITH_INFO, ret);
  } else {
    ASSERT_EQ(SQL_SUCCESS, ret);
  }

  EXPECT_GT(wcslen(info_value), 0);
}

// Driver Information

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoActiveEnvironments) {
  Validate(this->conn, SQL_ACTIVE_ENVIRONMENTS, static_cast<SQLUSMALLINT>(0));
}

#ifdef SQL_ASYNC_DBC_FUNCTIONS
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAsyncDbcFunctions) {
  Validate(this->conn, SQL_ASYNC_DBC_FUNCTIONS,
           static_cast<SQLUINTEGER>(SQL_ASYNC_DBC_NOT_CAPABLE));
}
#endif

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAsyncMode) {
  Validate(this->conn, SQL_ASYNC_MODE, static_cast<SQLUINTEGER>(SQL_AM_NONE));
}

#ifdef SQL_ASYNC_NOTIFICATION
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAsyncNotification) {
  Validate(this->conn, SQL_ASYNC_NOTIFICATION,
           static_cast<SQLUINTEGER>(SQL_ASYNC_NOTIFICATION_NOT_CAPABLE));
}
#endif

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoBatchRowCount) {
  Validate(this->conn, SQL_BATCH_ROW_COUNT, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoBatchSupport) {
  Validate(this->conn, SQL_BATCH_SUPPORT, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDataSourceName) {
  Validate(this->conn, SQL_DATA_SOURCE_NAME, (SQLWCHAR*)L"");
}

#ifdef SQL_DRIVER_AWARE_POOLING_SUPPORTED
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverAwarePoolingSupported) {
  // A driver does not need to implement SQL_DRIVER_AWARE_POOLING_SUPPORTED and the
  // Driver Manager will not honor to the driver's return value.

  Validate(this->conn, SQL_DRIVER_AWARE_POOLING_SUPPORTED,
           static_cast<SQLUINTEGER>(SQL_DRIVER_AWARE_POOLING_NOT_CAPABLE));
}
#endif

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverHdbc) {
  // Value returned from driver manager is the connection address
  ValidateGreaterThan(this->conn, SQL_DRIVER_HDBC, static_cast<SQLULEN>(0));
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverHdesc) {
  SQLHDESC descriptor;

  // Allocate a descriptor using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DESC, this->conn, &descriptor));

  // Value returned from driver manager is the desc address
  SQLHDESC local_desc = descriptor;
  SQLRETURN ret = SQLGetInfo(this->conn, SQL_HANDLE_DESC, &local_desc, 0, 0);
  EXPECT_EQ(SQL_SUCCESS, ret);
  EXPECT_GT(local_desc, static_cast<SQLHSTMT>(0));

  // Free descriptor handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DESC, descriptor));
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverHenv) {
  // Value returned from driver manager is the env address
  ValidateGreaterThan(this->conn, SQL_DRIVER_HENV, static_cast<SQLULEN>(0));
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverHlib) {
  ValidateGreaterThan(this->conn, SQL_DRIVER_HLIB, static_cast<SQLULEN>(0));
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverHstmt) {
  // Value returned from driver manager is the stmt address
  SQLHSTMT local_stmt = this->stmt;
  ASSERT_EQ(SQL_SUCCESS, SQLGetInfo(this->conn, SQL_DRIVER_HSTMT, &local_stmt, 0, 0));
  EXPECT_GT(local_stmt, static_cast<SQLHSTMT>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverName) {
  Validate(this->conn, SQL_DRIVER_NAME, (SQLWCHAR*)L"Arrow Flight ODBC Driver");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverOdbcVer) {
  Validate(this->conn, SQL_DRIVER_ODBC_VER, (SQLWCHAR*)L"03.80");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverVer) {
  Validate(this->conn, SQL_DRIVER_VER, (SQLWCHAR*)L"00.09.0000.0");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDynamicCursorAttributes1) {
  Validate(this->conn, SQL_DYNAMIC_CURSOR_ATTRIBUTES1, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDynamicCursorAttributes2) {
  Validate(this->conn, SQL_DYNAMIC_CURSOR_ATTRIBUTES2, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoForwardOnlyCursorAttributes1) {
  Validate(this->conn, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1,
           static_cast<SQLUINTEGER>(SQL_CA1_NEXT));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoForwardOnlyCursorAttributes2) {
  Validate(this->conn, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2,
           static_cast<SQLUINTEGER>(SQL_CA2_READ_ONLY_CONCURRENCY));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoFileUsage) {
  Validate(this->conn, SQL_FILE_USAGE, static_cast<SQLUSMALLINT>(SQL_FILE_NOT_SUPPORTED));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoGetDataExtensions) {
  Validate(this->conn, SQL_GETDATA_EXTENSIONS,
           static_cast<SQLUINTEGER>(SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoSchemaViews) {
  Validate(this->conn, SQL_INFO_SCHEMA_VIEWS,
           static_cast<SQLUINTEGER>(SQL_ISV_TABLES | SQL_ISV_COLUMNS | SQL_ISV_VIEWS));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoKeysetCursorAttributes1) {
  Validate(this->conn, SQL_KEYSET_CURSOR_ATTRIBUTES1, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoKeysetCursorAttributes2) {
  Validate(this->conn, SQL_KEYSET_CURSOR_ATTRIBUTES2, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxAsyncConcurrentStatements) {
  Validate(this->conn, SQL_MAX_ASYNC_CONCURRENT_STATEMENTS, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxConcurrentActivities) {
  Validate(this->conn, SQL_MAX_CONCURRENT_ACTIVITIES, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxDriverConnections) {
  Validate(this->conn, SQL_MAX_DRIVER_CONNECTIONS, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoOdbcInterfaceConformance) {
  Validate(this->conn, SQL_ODBC_INTERFACE_CONFORMANCE,
           static_cast<SQLUINTEGER>(SQL_OIC_CORE));
}

// case SQL_ODBC_STANDARD_CLI_CONFORMANCE: - mentioned in SQLGetInfo spec with no
// description and there is no constant for this.
TYPED_TEST(FlightSQLODBCTestBase, DISABLED_TestSQLGetInfoOdbcStandardCliConformance) {
  // Type commented out in odbc_connection.cc
  // Type does not exist in sql.h
  // Validate(this->conn, SQL_ODBC_STANDARD_CLI_CONFORMANCE,
  // static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoOdbcVer) {
  // This is implemented only in the Driver Manager.

  Validate(this->conn, SQL_ODBC_VER, (SQLWCHAR*)L"03.80.0000");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoParamArrayRowCounts) {
  Validate(this->conn, SQL_PARAM_ARRAY_ROW_COUNTS,
           static_cast<SQLUINTEGER>(SQL_PARC_NO_BATCH));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoParamArraySelects) {
  Validate(this->conn, SQL_PARAM_ARRAY_SELECTS,
           static_cast<SQLUINTEGER>(SQL_PAS_NO_SELECT));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoRowUpdates) {
  Validate(this->conn, SQL_ROW_UPDATES, (SQLWCHAR*)L"N");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoSearchPatternEscape) {
  Validate(this->conn, SQL_SEARCH_PATTERN_ESCAPE, (SQLWCHAR*)L"\\");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoServerName) {
  ValidateNotEmptySQLWCHAR(this->conn, SQL_SERVER_NAME, false);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoStaticCursorAttributes1) {
  Validate(this->conn, SQL_STATIC_CURSOR_ATTRIBUTES1, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoStaticCursorAttributes2) {
  Validate(this->conn, SQL_STATIC_CURSOR_ATTRIBUTES2, static_cast<SQLUINTEGER>(0));
}

// DBMS Product Information

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDatabaseName) {
  Validate(this->conn, SQL_DATABASE_NAME, (SQLWCHAR*)L"");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDbmsName) {
  ValidateNotEmptySQLWCHAR(this->conn, SQL_DBMS_NAME, false);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDbmsVer) {
  ValidateNotEmptySQLWCHAR(this->conn, SQL_DBMS_VER, false);
}

// Data Source Information

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAccessibleProcedures) {
  Validate(this->conn, SQL_ACCESSIBLE_PROCEDURES, (SQLWCHAR*)L"N");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAccessibleTables) {
  Validate(this->conn, SQL_ACCESSIBLE_TABLES, (SQLWCHAR*)L"Y");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoBookmarkPersistence) {
  Validate(this->conn, SQL_BOOKMARK_PERSISTENCE, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCatalogTerm) {
  Validate(this->conn, SQL_CATALOG_TERM, (SQLWCHAR*)L"");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCollationSeq) {
  Validate(this->conn, SQL_COLLATION_SEQ, (SQLWCHAR*)L"");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConcatNullBehavior) {
  Validate(this->conn, SQL_CONCAT_NULL_BEHAVIOR, static_cast<SQLUSMALLINT>(SQL_CB_NULL));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCursorCommitBehavior) {
  Validate(this->conn, SQL_CURSOR_COMMIT_BEHAVIOR,
           static_cast<SQLUSMALLINT>(SQL_CB_CLOSE));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCursorRollbackBehavior) {
  Validate(this->conn, SQL_CURSOR_ROLLBACK_BEHAVIOR,
           static_cast<SQLUSMALLINT>(SQL_CB_CLOSE));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCursorSensitivity) {
  Validate(this->conn, SQL_CURSOR_SENSITIVITY, static_cast<SQLUINTEGER>(SQL_UNSPECIFIED));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDataSourceReadOnly) {
  Validate(this->conn, SQL_DATA_SOURCE_READ_ONLY, (SQLWCHAR*)L"N");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDefaultTxnIsolation) {
  Validate(this->conn, SQL_DEFAULT_TXN_ISOLATION, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDescribeParameter) {
  Validate(this->conn, SQL_DESCRIBE_PARAMETER, (SQLWCHAR*)L"N");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMultResultSets) {
  Validate(this->conn, SQL_MULT_RESULT_SETS, (SQLWCHAR*)L"N");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMultipleActiveTxn) {
  Validate(this->conn, SQL_MULTIPLE_ACTIVE_TXN, (SQLWCHAR*)L"N");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoNeedLongDataLen) {
  Validate(this->conn, SQL_NEED_LONG_DATA_LEN, (SQLWCHAR*)L"N");
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoNullCollation) {
  Validate(this->conn, SQL_NULL_COLLATION, static_cast<SQLUSMALLINT>(SQL_NC_START));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoProcedureTerm) {
  Validate(this->conn, SQL_PROCEDURE_TERM, (SQLWCHAR*)L"");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoSchemaTerm) {
  Validate(this->conn, SQL_SCHEMA_TERM, (SQLWCHAR*)L"schema");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoScrollOptions) {
  Validate(this->conn, SQL_SCROLL_OPTIONS, static_cast<SQLUINTEGER>(SQL_SO_FORWARD_ONLY));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoTableTerm) {
  Validate(this->conn, SQL_TABLE_TERM, (SQLWCHAR*)L"table");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoTxnCapable) {
  Validate(this->conn, SQL_TXN_CAPABLE, static_cast<SQLUSMALLINT>(SQL_TC_NONE));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoTxnIsolationOption) {
  Validate(this->conn, SQL_TXN_ISOLATION_OPTION, static_cast<SQLUINTEGER>(0));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoUserName) {
  Validate(this->conn, SQL_USER_NAME, (SQLWCHAR*)L"");
}

// Supported SQL

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAggregateFunctions) {
  Validate(
      this->conn, SQL_AGGREGATE_FUNCTIONS,
      static_cast<SQLUINTEGER>(SQL_AF_ALL | SQL_AF_AVG | SQL_AF_COUNT | SQL_AF_DISTINCT |
                               SQL_AF_MAX | SQL_AF_MIN | SQL_AF_SUM));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAlterDomain) {
  Validate(this->conn, SQL_ALTER_DOMAIN, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, DISABLED_TestSQLGetInfoAlterSchema) {
  // Type commented out in odbc_connection.cc
  // Type does not exist in sql.h
  // Validate(this->conn, SQL_ALTER_SCHEMA, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAlterTable) {
  Validate(this->conn, SQL_ALTER_TABLE, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, DISABLED_TestSQLGetInfoAnsiSqlDatetimeLiterals) {
  // Type commented out in odbc_connection.cc
  // Type does not exist in sql.h
  // Validate(this->conn, SQL_ANSI_SQL_DATETIME_LITERALS, (SQLWCHAR*)L"");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCatalogLocation) {
  Validate(this->conn, SQL_CATALOG_LOCATION, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCatalogName) {
  Validate(this->conn, SQL_CATALOG_NAME, (SQLWCHAR*)L"N");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCatalogNameSeparator) {
  Validate(this->conn, SQL_CATALOG_NAME_SEPARATOR, (SQLWCHAR*)L"");
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoCatalogUsage) {
  Validate(this->conn, SQL_CATALOG_USAGE, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoColumnAlias) {
  Validate(this->conn, SQL_COLUMN_ALIAS, (SQLWCHAR*)L"Y");
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoCorrelationName) {
  Validate(this->conn, SQL_CORRELATION_NAME, static_cast<SQLUSMALLINT>(SQL_CN_NONE));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCreateAssertion) {
  Validate(this->conn, SQL_CREATE_ASSERTION, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCreateCharacterSet) {
  Validate(this->conn, SQL_CREATE_CHARACTER_SET, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCreateCollation) {
  Validate(this->conn, SQL_CREATE_COLLATION, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCreateDomain) {
  Validate(this->conn, SQL_CREATE_DOMAIN, static_cast<SQLUINTEGER>(0));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoCreateSchema) {
  Validate(this->conn, SQL_CREATE_SCHEMA, static_cast<SQLUINTEGER>(1));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoCreateTable) {
  Validate(this->conn, SQL_CREATE_TABLE, static_cast<SQLUINTEGER>(1));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCreateTranslation) {
  Validate(this->conn, SQL_CREATE_TRANSLATION, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDdlIndex) {
  Validate(this->conn, SQL_DDL_INDEX, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropAssertion) {
  Validate(this->conn, SQL_DROP_ASSERTION, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropCharacterSet) {
  Validate(this->conn, SQL_DROP_CHARACTER_SET, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropCollation) {
  Validate(this->conn, SQL_DROP_COLLATION, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropDomain) {
  Validate(this->conn, SQL_DROP_DOMAIN, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropSchema) {
  Validate(this->conn, SQL_DROP_SCHEMA, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropTable) {
  Validate(this->conn, SQL_DROP_TABLE, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropTranslation) {
  Validate(this->conn, SQL_DROP_TRANSLATION, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropView) {
  Validate(this->conn, SQL_DROP_VIEW, static_cast<SQLUINTEGER>(0));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoExpressionsInOrderby) {
  Validate(this->conn, SQL_EXPRESSIONS_IN_ORDERBY, (SQLWCHAR*)L"N");
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoGroupBy) {
  Validate(this->conn, SQL_GROUP_BY,
           static_cast<SQLUSMALLINT>(SQL_GB_GROUP_BY_CONTAINS_SELECT));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoIdentifierCase) {
  Validate(this->conn, SQL_IDENTIFIER_CASE, static_cast<SQLUSMALLINT>(SQL_IC_MIXED));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoIdentifierQuoteChar) {
  Validate(this->conn, SQL_IDENTIFIER_QUOTE_CHAR, (SQLWCHAR*)L"\"");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoIndexKeywords) {
  Validate(this->conn, SQL_INDEX_KEYWORDS, static_cast<SQLUINTEGER>(SQL_IK_NONE));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoInsertStatement) {
  Validate(this->conn, SQL_INSERT_STATEMENT,
           static_cast<SQLUINTEGER>(SQL_IS_INSERT_LITERALS | SQL_IS_INSERT_SEARCHED |
                                    SQL_IS_SELECT_INTO));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoIntegrity) {
  Validate(this->conn, SQL_INTEGRITY, (SQLWCHAR*)L"N");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoKeywords) {
  ValidateNotEmptySQLWCHAR(this->conn, SQL_KEYWORDS, true);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoLikeEscapeClause) {
  Validate(this->conn, SQL_LIKE_ESCAPE_CLAUSE, (SQLWCHAR*)L"Y");
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoNonNullableColumns) {
  Validate(this->conn, SQL_NON_NULLABLE_COLUMNS, static_cast<SQLUSMALLINT>(SQL_NNC_NULL));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoOjCapabilities) {
  Validate(this->conn, SQL_OJ_CAPABILITIES,
           static_cast<SQLUINTEGER>(SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_FULL));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoOrderByColumnsInSelect) {
  Validate(this->conn, SQL_ORDER_BY_COLUMNS_IN_SELECT, (SQLWCHAR*)L"Y");
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoOuterJoins) {
  Validate(this->conn, SQL_OUTER_JOINS, (SQLWCHAR*)L"N");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoProcedures) {
  Validate(this->conn, SQL_PROCEDURES, (SQLWCHAR*)L"N");
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoQuotedIdentifierCase) {
  Validate(this->conn, SQL_QUOTED_IDENTIFIER_CASE,
           static_cast<SQLUSMALLINT>(SQL_IC_MIXED));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoSchemaUsage) {
  Validate(this->conn, SQL_SCHEMA_USAGE, static_cast<SQLUINTEGER>(SQL_SU_DML_STATEMENTS));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoSpecialCharacters) {
  Validate(this->conn, SQL_SPECIAL_CHARACTERS, (SQLWCHAR*)L"");
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoSqlConformance) {
  Validate(this->conn, SQL_SQL_CONFORMANCE, static_cast<SQLUINTEGER>(SQL_SC_SQL92_ENTRY));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoSubqueries) {
  Validate(this->conn, SQL_SUBQUERIES,
           static_cast<SQLUINTEGER>(SQL_SQ_CORRELATED_SUBQUERIES | SQL_SQ_COMPARISON |
                                    SQL_SQ_EXISTS | SQL_SQ_IN | SQL_SQ_QUANTIFIED));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoUnion) {
  Validate(this->conn, SQL_UNION,
           static_cast<SQLUINTEGER>(SQL_U_UNION | SQL_U_UNION_ALL));
}

// SQL Limits

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxBinaryLiteralLen) {
  Validate(this->conn, SQL_MAX_BINARY_LITERAL_LEN, static_cast<SQLUINTEGER>(0));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxCatalogNameLen) {
  Validate(this->conn, SQL_MAX_CATALOG_NAME_LEN, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxCharLiteralLen) {
  Validate(this->conn, SQL_MAX_CHAR_LITERAL_LEN, static_cast<SQLUINTEGER>(0));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxColumnNameLen) {
  Validate(this->conn, SQL_MAX_COLUMN_NAME_LEN, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxColumnsInGroupBy) {
  Validate(this->conn, SQL_MAX_COLUMNS_IN_GROUP_BY, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxColumnsInIndex) {
  Validate(this->conn, SQL_MAX_COLUMNS_IN_INDEX, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxColumnsInOrderBy) {
  Validate(this->conn, SQL_MAX_COLUMNS_IN_ORDER_BY, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxColumnsInSelect) {
  Validate(this->conn, SQL_MAX_COLUMNS_IN_SELECT, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxColumnsInTable) {
  Validate(this->conn, SQL_MAX_COLUMNS_IN_TABLE, static_cast<SQLUSMALLINT>(0));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxCursorNameLen) {
  Validate(this->conn, SQL_MAX_CURSOR_NAME_LEN, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxIdentifierLen) {
  Validate(this->conn, SQL_MAX_IDENTIFIER_LEN, static_cast<SQLUSMALLINT>(65535));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxIndexSize) {
  Validate(this->conn, SQL_MAX_INDEX_SIZE, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxProcedureNameLen) {
  Validate(this->conn, SQL_MAX_PROCEDURE_NAME_LEN, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxRowSize) {
  Validate(this->conn, SQL_MAX_ROW_SIZE, (SQLWCHAR*)L"");
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxRowSizeIncludesLong) {
  Validate(this->conn, SQL_MAX_ROW_SIZE_INCLUDES_LONG, (SQLWCHAR*)L"N");
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxSchemaNameLen) {
  Validate(this->conn, SQL_MAX_SCHEMA_NAME_LEN, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxStatementLen) {
  Validate(this->conn, SQL_MAX_STATEMENT_LEN, static_cast<SQLUINTEGER>(0));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxTableNameLen) {
  Validate(this->conn, SQL_MAX_TABLE_NAME_LEN, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxTablesInSelect) {
  Validate(this->conn, SQL_MAX_TABLES_IN_SELECT, static_cast<SQLUSMALLINT>(0));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxUserNameLen) {
  Validate(this->conn, SQL_MAX_USER_NAME_LEN, static_cast<SQLUSMALLINT>(0));
}

// Scalar Function Information

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertFunctions) {
  Validate(this->conn, SQL_CONVERT_FUNCTIONS, static_cast<SQLUINTEGER>(0));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoNumericFunctions) {
  Validate(this->conn, SQL_NUMERIC_FUNCTIONS, static_cast<SQLUINTEGER>(4058942));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoStringFunctions) {
  Validate(this->conn, SQL_STRING_FUNCTIONS,
           static_cast<SQLUINTEGER>(SQL_FN_STR_LTRIM | SQL_FN_STR_LENGTH |
                                    SQL_FN_STR_REPLACE | SQL_FN_STR_RTRIM));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoSystemFunctions) {
  Validate(this->conn, SQL_SYSTEM_FUNCTIONS,
           static_cast<SQLUINTEGER>(SQL_FN_SYS_IFNULL | SQL_FN_SYS_USERNAME));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoTimedateAddIntervals) {
  Validate(this->conn, SQL_TIMEDATE_ADD_INTERVALS,
           static_cast<SQLUINTEGER>(SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND |
                                    SQL_FN_TSI_MINUTE | SQL_FN_TSI_HOUR | SQL_FN_TSI_DAY |
                                    SQL_FN_TSI_WEEK | SQL_FN_TSI_MONTH |
                                    SQL_FN_TSI_QUARTER | SQL_FN_TSI_YEAR));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoTimedateDiffIntervals) {
  Validate(this->conn, SQL_TIMEDATE_DIFF_INTERVALS,
           static_cast<SQLUINTEGER>(SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND |
                                    SQL_FN_TSI_MINUTE | SQL_FN_TSI_HOUR | SQL_FN_TSI_DAY |
                                    SQL_FN_TSI_WEEK | SQL_FN_TSI_MONTH |
                                    SQL_FN_TSI_QUARTER | SQL_FN_TSI_YEAR));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoTimedateFunctions) {
  Validate(this->conn, SQL_TIMEDATE_FUNCTIONS,
           static_cast<SQLUINTEGER>(
               SQL_FN_TD_CURRENT_DATE | SQL_FN_TD_CURRENT_TIME |
               SQL_FN_TD_CURRENT_TIMESTAMP | SQL_FN_TD_CURDATE | SQL_FN_TD_CURTIME |
               SQL_FN_TD_DAYNAME | SQL_FN_TD_DAYOFMONTH | SQL_FN_TD_DAYOFWEEK |
               SQL_FN_TD_DAYOFYEAR | SQL_FN_TD_EXTRACT | SQL_FN_TD_HOUR |
               SQL_FN_TD_MINUTE | SQL_FN_TD_MONTH | SQL_FN_TD_MONTHNAME | SQL_FN_TD_NOW |
               SQL_FN_TD_QUARTER | SQL_FN_TD_SECOND | SQL_FN_TD_TIMESTAMPADD |
               SQL_FN_TD_TIMESTAMPDIFF | SQL_FN_TD_WEEK | SQL_FN_TD_YEAR));
}

// Conversion Information

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertBigint) {
  Validate(this->conn, SQL_CONVERT_BIGINT, static_cast<SQLUINTEGER>(8));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertBinary) {
  Validate(this->conn, SQL_CONVERT_BINARY, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertBit) {
  Validate(this->conn, SQL_CONVERT_BIT, static_cast<SQLUINTEGER>(0));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertChar) {
  Validate(this->conn, SQL_CONVERT_CHAR, static_cast<SQLUINTEGER>(0));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertDate) {
  Validate(this->conn, SQL_CONVERT_DATE, static_cast<SQLUINTEGER>(0));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertDecimal) {
  Validate(this->conn, SQL_CONVERT_DECIMAL, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertDouble) {
  Validate(this->conn, SQL_CONVERT_DOUBLE, static_cast<SQLUINTEGER>(0));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertFloat) {
  Validate(this->conn, SQL_CONVERT_FLOAT, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertInteger) {
  Validate(this->conn, SQL_CONVERT_INTEGER, static_cast<SQLUINTEGER>(0));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertIntervalDayTime) {
  Validate(this->conn, SQL_CONVERT_INTERVAL_DAY_TIME, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertIntervalYearMonth) {
  Validate(this->conn, SQL_CONVERT_INTERVAL_YEAR_MONTH, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertLongvarbinary) {
  Validate(this->conn, SQL_CONVERT_LONGVARBINARY, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertLongvarchar) {
  Validate(this->conn, SQL_CONVERT_LONGVARCHAR, static_cast<SQLUINTEGER>(0));
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertNumeric) {
  Validate(this->conn, SQL_CONVERT_NUMERIC, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertReal) {
  Validate(this->conn, SQL_CONVERT_REAL, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertSmallint) {
  Validate(this->conn, SQL_CONVERT_SMALLINT, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertTime) {
  Validate(this->conn, SQL_CONVERT_TIME, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertTimestamp) {
  Validate(this->conn, SQL_CONVERT_TIMESTAMP, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertTinyint) {
  Validate(this->conn, SQL_CONVERT_TINYINT, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertVarbinary) {
  Validate(this->conn, SQL_CONVERT_VARBINARY, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertVarchar) {
  Validate(this->conn, SQL_CONVERT_VARCHAR, static_cast<SQLUINTEGER>(0));
}

}  // namespace arrow::flight::sql::odbc
