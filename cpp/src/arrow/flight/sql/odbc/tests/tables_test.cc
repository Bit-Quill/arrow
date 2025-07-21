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

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_statement.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/statement.h"

#ifdef _WIN32
#  include <windows.h>
#endif

#include <sql.h>
#include <sqltypes.h>
#include <sqlucode.h>

#include "gtest/gtest.h"

namespace arrow::flight::sql::odbc {

// TODO: Add tests with SQLDescribeCol to check metadata of SQLColumns for ODBC 2 and
// ODBC 3.

// Helper Functions

void CheckStringColumnW(SQLHSTMT stmt, int colId, const std::wstring& expected) {
  SQLWCHAR buf[1024];
  SQLLEN bufLen = sizeof(buf);

  SQLRETURN ret = SQLGetData(stmt, colId, SQL_C_WCHAR, buf, bufLen, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // TODO REMOVE
  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt) << std::endl;
  }

  if (bufLen > 0) {
    // returned bufLen is in bytes so convert to length in characters
    size_t charCount = static_cast<size_t>(bufLen) / ODBC::GetSqlWCharSize();
    std::wstring returned(buf, buf + charCount);

    std::wcerr << L"Comparing returned string:'" << returned << L"' to expected string:"
               << expected << std::endl;

    EXPECT_EQ(returned, expected);
  } else {
    EXPECT_TRUE(expected.empty());
  }
}

void CheckNullColumnW(SQLHSTMT stmt, int colId) {
  SQLWCHAR buf[1024];
  SQLLEN bufLen = sizeof(buf);

  SQLRETURN ret = SQLGetData(stmt, colId, SQL_C_WCHAR, buf, bufLen, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // TODO REMOVE
  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt) << std::endl;
  }

  EXPECT_EQ(bufLen, SQL_NULL_DATA);
}

bool ValidateFetch(SQLHSTMT stmt, SQLRETURN expectedReturn) {
  SQLRETURN ret = SQLFetch(stmt);

  EXPECT_EQ(ret, expectedReturn);
  if (ret != SQL_SUCCESS) {
    // TODO REMOVE
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt) << std::endl;
    return false;
  }

  return true;
}

// Test Cases

TEST_F(FlightSQLODBCMockTestBase, SQLTablesTestInputData) {
  this->connect();
  this->CreateTestTables();

  SQLWCHAR catalogName[] = L"";
  SQLWCHAR schemaName[] = L"";
  SQLWCHAR tableName[] = L"";
  SQLWCHAR tableType[] = L"";

  // All values populated
  SQLRETURN ret = SQLTables(this->stmt, catalogName, sizeof(catalogName), schemaName,
                            sizeof(schemaName), tableName, sizeof(tableName), tableType,
                            sizeof(tableType));

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Sizes are nulls
  ret = SQLTables(this->stmt, catalogName, 0, schemaName, 0, tableName, 0, tableType, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Values are nulls
  ret = SQLTables(this->stmt, 0, sizeof(catalogName), 0, sizeof(schemaName), 0,
                  sizeof(tableName), 0, sizeof(tableType));

  EXPECT_EQ(ret, SQL_SUCCESS);

  // All values and sizes are nulls
  ret = SQLTables(this->stmt, 0, 0, 0, 0, 0, 0, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLTablesTestGetMetadataForAllCatalogs) {
  this->connect();
  this->CreateTestTables();

  SQLWCHAR empty[] = L"";
  SQLWCHAR SQL_ALL_CATALOGS_W[] = L"%";

  std::wstring expectedName = std::wstring(L"main");

  // Get Catalog metadata
  SQLRETURN ret = SQLTables(this->stmt, SQL_ALL_CATALOGS_W, SQL_NTS, empty, SQL_NTS,
                            empty, SQL_NTS, empty, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_SUCCESS);

  CheckStringColumnW(this->stmt, 1, expectedName);
  CheckNullColumnW(this->stmt, 2);
  CheckNullColumnW(this->stmt, 3);
  CheckNullColumnW(this->stmt, 4);
  CheckNullColumnW(this->stmt, 5);

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLTablesTestGetMetadataForNamedCatalog) {
  this->connect();
  this->CreateTestTables();

  SQLWCHAR catalogName[] = L"main";
  SQLWCHAR* tableNames[] = {(SQLWCHAR*)L"TestTable", (SQLWCHAR*)L"foreignTable",
                            (SQLWCHAR*)L"intTable", (SQLWCHAR*)L"sqlite_sequence"};
  std::wstring expectedCatalogName = std::wstring(L"main");
  std::wstring expectedTableType = std::wstring(L"table");

  // Get named Catalog metadata - Mock server returns the system table sqlite_sequence as
  // type "table"
  SQLRETURN ret = SQLTables(this->stmt, catalogName, SQL_NTS, nullptr, SQL_NTS, nullptr,
                            SQL_NTS, nullptr, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  for (size_t i = 0; i < sizeof(tableNames) / sizeof(*tableNames); ++i) {
    ValidateFetch(this->stmt, SQL_SUCCESS);

    CheckStringColumnW(this->stmt, 1, expectedCatalogName);
    // Mock server does not support table schema
    CheckNullColumnW(this->stmt, 2);
    CheckStringColumnW(this->stmt, 3, tableNames[i]);
    CheckStringColumnW(this->stmt, 4, expectedTableType);
    CheckNullColumnW(this->stmt, 5);
  }

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLTablesTestGetSchemaHasNoData) {
  this->connect();
  this->CreateTestTables();

  SQLWCHAR SQL_ALL_SCHEMAS_W[] = L"%";

  // Validate that no schema data is available for Mock server
  SQLRETURN ret = SQLTables(this->stmt, nullptr, SQL_NTS, SQL_ALL_SCHEMAS_W, SQL_NTS,
                            nullptr, SQL_NTS, nullptr, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLTablesTestGetMetadataForAllTables) {
  this->connect();
  this->CreateTestTables();

  SQLWCHAR SQL_ALL_TABLES_W[] = L"%";
  SQLWCHAR* tableNames[] = {(SQLWCHAR*)L"TestTable", (SQLWCHAR*)L"foreignTable",
                            (SQLWCHAR*)L"intTable", (SQLWCHAR*)L"sqlite_sequence"};
  std::wstring expectedCatalogName = std::wstring(L"main");
  std::wstring expectedTableType = std::wstring(L"table");

  // Get all Table metadata - Mock server returns the system table sqlite_sequence as type
  // "table"
  SQLRETURN ret = SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                            SQL_ALL_TABLES_W, SQL_NTS, nullptr, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  for (size_t i = 0; i < sizeof(tableNames) / sizeof(*tableNames); ++i) {
    ValidateFetch(this->stmt, SQL_SUCCESS);

    CheckStringColumnW(this->stmt, 1, expectedCatalogName);
    // Mock server does not support table schema
    CheckNullColumnW(this->stmt, 2);
    CheckStringColumnW(this->stmt, 3, tableNames[i]);
    CheckStringColumnW(this->stmt, 4, expectedTableType);
    CheckNullColumnW(this->stmt, 5);
  }

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, SQLTablesTestGetMetadataForSystemTables) {
  this->connect();

  SQLWCHAR SQL_ALL_TABLES_W[] = L"%";
  SQLWCHAR* schemaNames[] = {(SQLWCHAR*)L"INFORMATION_SCHEMA",
                             (SQLWCHAR*)L"INFORMATION_SCHEMA",
                             (SQLWCHAR*)L"INFORMATION_SCHEMA",
                             (SQLWCHAR*)L"INFORMATION_SCHEMA",
                             (SQLWCHAR*)L"INFORMATION_SCHEMA",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys.cache",
                             (SQLWCHAR*)L"sys.cache",
                             (SQLWCHAR*)L"sys.cache",
                             (SQLWCHAR*)L"sys.cache"};
  SQLWCHAR* tableNames[] = {
      (SQLWCHAR*)L"CATALOGS",        (SQLWCHAR*)L"COLUMNS",
      (SQLWCHAR*)L"SCHEMATA",        (SQLWCHAR*)L"TABLES",
      (SQLWCHAR*)L"VIEWS",           (SQLWCHAR*)L"boot",
      (SQLWCHAR*)L"fragments",       (SQLWCHAR*)L"jobs",
      (SQLWCHAR*)L"jobs_recent",     (SQLWCHAR*)L"materializations",
      (SQLWCHAR*)L"membership",      (SQLWCHAR*)L"memory",
      (SQLWCHAR*)L"nodes",           (SQLWCHAR*)L"options",
      (SQLWCHAR*)L"privileges",      (SQLWCHAR*)L"reflection_dependencies",
      (SQLWCHAR*)L"reflections",     (SQLWCHAR*)L"refreshes",
      (SQLWCHAR*)L"roles",           (SQLWCHAR*)L"services",
      (SQLWCHAR*)L"slicing_threads", (SQLWCHAR*)L"table_statistics",
      (SQLWCHAR*)L"threads",         (SQLWCHAR*)L"timezone_abbrevs",
      (SQLWCHAR*)L"timezone_names",  (SQLWCHAR*)L"user_defined_functions",
      (SQLWCHAR*)L"version",         (SQLWCHAR*)L"datasets",
      (SQLWCHAR*)L"mount_points",    (SQLWCHAR*)L"objects",
      (SQLWCHAR*)L"storage_plugins"};
  std::wstring expectedTableType = std::wstring(L"SYSTEM_TABLE");

  //  Get all Table metadata including System tables
  SQLRETURN ret = SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                            SQL_ALL_TABLES_W, SQL_NTS, nullptr, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  for (size_t i = 0; i < sizeof(tableNames) / sizeof(*tableNames); ++i) {
    ValidateFetch(this->stmt, SQL_SUCCESS);

    CheckNullColumnW(this->stmt, 1);
    CheckStringColumnW(this->stmt, 2, schemaNames[i]);
    CheckStringColumnW(this->stmt, 3, tableNames[i]);
    CheckStringColumnW(this->stmt, 4, expectedTableType);
    CheckNullColumnW(this->stmt, 5);
  }

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLTablesTestGetMetadataForTableName) {
  this->connect();
  this->CreateTestTables();

  SQLWCHAR SQL_ALL_TABLES_W[] = L"%";
  SQLWCHAR* tableNames[] = {(SQLWCHAR*)L"TestTable", (SQLWCHAR*)L"foreignTable",
                            (SQLWCHAR*)L"intTable", (SQLWCHAR*)L"sqlite_sequence"};
  std::wstring expectedCatalogName = std::wstring(L"main");
  std::wstring expectedTableType = std::wstring(L"table");

  for (size_t i = 0; i < sizeof(tableNames) / sizeof(*tableNames); ++i) {
    //  Get specific Table metadata
    SQLRETURN ret = SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                              tableNames[i], SQL_NTS, nullptr, SQL_NTS);

    EXPECT_EQ(ret, SQL_SUCCESS);

    ValidateFetch(this->stmt, SQL_SUCCESS);

    CheckStringColumnW(this->stmt, 1, expectedCatalogName);
    // Mock server does not support table schema
    CheckNullColumnW(this->stmt, 2);
    CheckStringColumnW(this->stmt, 3, tableNames[i]);
    CheckStringColumnW(this->stmt, 4, expectedTableType);
    CheckNullColumnW(this->stmt, 5);

    ValidateFetch(this->stmt, SQL_NO_DATA);
  }

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLTablesTestGetMetadataForBadTableNameNoData) {
  this->connect();
  this->CreateTestTables();

  SQLWCHAR BAD_TABLE_NAME[] = L"bad_table_name";

  //  Try to get metadata for invalid table name
  SQLRETURN ret = SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                            BAD_TABLE_NAME, SQL_NTS, nullptr, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLTablesGetMetadataForSpecificTableTypeHasNoData) {
  this->connect();
  this->CreateTestTables();

  SQLWCHAR empty[] = L"";
  SQLWCHAR table[] = L"TestTable";
  SQLWCHAR type[] = L"table";
  SQLRETURN ret = SQL_SUCCESS;

  // TODO Mock server doess not support filtering by table type
  ret = SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, empty, SQL_NTS, table,
                  SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLTablesGetMetadataForTableTypeHasNoData) {
  this->connect();
  this->CreateTestTables();

  SQLWCHAR empty[] = L"";
  SQLWCHAR table[] = L"TestTable";
  SQLWCHAR* typeLists[] = {(SQLWCHAR*)L"table", (SQLWCHAR*)L"'TABLE'",
                           (SQLWCHAR*)L"TABLE,VIEW"};
  SQLRETURN ret = SQL_SUCCESS;

  for (size_t i = 0; i < sizeof(typeLists) / sizeof(*typeLists); ++i) {
    // TODO Mock server doess not support filtering by table type
    ret = SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, empty, SQL_NTS,
                    typeLists[i], SQL_NTS);

    EXPECT_EQ(ret, SQL_SUCCESS);

    ValidateFetch(this->stmt, SQL_NO_DATA);
  }

  this->disconnect();
}

// TODO Will work once we have query to create remote table
TEST_F(FlightSQLODBCRemoteTestBase, SQLTablesGetMetadataForTableType) {
  GTEST_SKIP();
  this->connect();

  SQLWCHAR empty[] = L"";
  SQLWCHAR* typeLists[] = {(SQLWCHAR*)L"TABLE", (SQLWCHAR*)L"SYSTEM_TABLE",
                           (SQLWCHAR*)L"VIEW", (SQLWCHAR*)L"TABLE,VIEW"};
  SQLRETURN ret = SQL_SUCCESS;

  for (size_t i = 0; i < sizeof(typeLists) / sizeof(*typeLists); ++i) {
    ret = SQLTables(this->stmt, nullptr, SQL_NTS, empty, SQL_NTS, empty, SQL_NTS,
                    typeLists[i], SQL_NTS);

    EXPECT_EQ(ret, SQL_SUCCESS);

    ValidateFetch(this->stmt, SQL_SUCCESS);
  }

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLTablesGetSupportedTableTypes) {
  this->connect();

  SQLWCHAR empty[] = L"";
  SQLWCHAR SQL_ALL_TABLE_TYPES_W[] = L"%";
  std::wstring expectedTableType = std::wstring(L"table");

  // Mock returns lower case for supported type of "table"
  SQLRETURN ret = SQLTables(this->stmt, empty, SQL_NTS, empty, SQL_NTS, empty, SQL_NTS,
                            SQL_ALL_TABLE_TYPES_W, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_SUCCESS);

  CheckNullColumnW(this->stmt, 1);
  CheckNullColumnW(this->stmt, 2);
  CheckNullColumnW(this->stmt, 3);
  CheckStringColumnW(this->stmt, 4, expectedTableType);
  CheckNullColumnW(this->stmt, 5);

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, SQLTablesGetSupportedTableTypes) {
  this->connect();

  SQLWCHAR empty[] = L"";
  SQLWCHAR SQL_ALL_TABLE_TYPES_W[] = L"%";
  SQLWCHAR* typeLists[] = {(SQLWCHAR*)L"TABLE", (SQLWCHAR*)L"SYSTEM_TABLE",
                           (SQLWCHAR*)L"VIEW"};

  SQLRETURN ret = SQLTables(this->stmt, empty, SQL_NTS, empty, SQL_NTS, empty, SQL_NTS,
                            SQL_ALL_TABLE_TYPES_W, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  for (size_t i = 0; i < sizeof(typeLists) / sizeof(*typeLists); ++i) {
    ValidateFetch(this->stmt, SQL_SUCCESS);

    CheckNullColumnW(this->stmt, 1);
    CheckNullColumnW(this->stmt, 2);
    CheckNullColumnW(this->stmt, 3);
    CheckStringColumnW(this->stmt, 4, typeLists[i]);
    CheckNullColumnW(this->stmt, 5);
  }

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

}  // namespace arrow::flight::sql::odbc
