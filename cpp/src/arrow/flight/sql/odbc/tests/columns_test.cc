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
void checkSQLColumns(
    SQLHSTMT stmt, const std::wstring& expectedCatalog, const std::wstring& expectedTable,
    const std::wstring& expectedColumn, const SQLINTEGER& expectedDataType,
    const std::wstring& expectedTypeName, const SQLINTEGER& expectedColumnSize,
    const SQLINTEGER& expectedBufferLength, const SQLSMALLINT& expectedDecimalDigits,
    const SQLSMALLINT& expectedNumPrecRadix, const SQLSMALLINT& expectedNullable,
    const SQLSMALLINT& expectedSqlDataType, const SQLSMALLINT& expectedDateTimeSub,
    const SQLINTEGER& expectedOctetCharLength, const SQLINTEGER& expectedOrdinalPosition,
    const std::wstring& expectedIsNullable) {
  CheckStringColumnW(stmt, 1, expectedCatalog);  // catalog
  CheckNullColumnW(stmt, 2);                     // schema
  CheckStringColumnW(stmt, 3, expectedTable);    // table name
  CheckStringColumnW(stmt, 4, expectedColumn);   // column name

  CheckIntColumn(stmt, 5, expectedDataType);  // data type

  CheckStringColumnW(stmt, 6, expectedTypeName);  // type name

  // mock limitation: SQLite mock server returns 10 for bigint size when spec indicates
  // should be 19
  CheckIntColumn(stmt, 7, expectedColumnSize);    // column size
  CheckIntColumn(stmt, 8, expectedBufferLength);  // buffer length

  // DECIMAL_DIGITS should be 0 for bigint type since it is exact
  // mock limitation: SQLite mock server returns 10 for bigint decimal digits when spec
  // indicates should be 0
  CheckSmallIntColumn(stmt, 9, expectedDecimalDigits);  // decimal digits
  CheckSmallIntColumn(stmt, 10, expectedNumPrecRadix);  // num prec radix
  CheckSmallIntColumn(stmt, 11,
                      expectedNullable);  // nullable

  CheckNullColumnW(stmt, 12);  // remarks
  CheckNullColumnW(stmt, 13);  // column def

  CheckSmallIntColumn(stmt, 14, expectedSqlDataType);  // sql data type  not NULL
  CheckSmallIntColumn(stmt, 15, expectedDateTimeSub);  // sql date type sub
  CheckIntColumn(stmt, 16, expectedOctetCharLength);   // char octet length
  CheckIntColumn(stmt, 17,
                 expectedOrdinalPosition);  // oridinal position

  CheckStringColumnW(stmt, 18, expectedIsNullable);  // is nullable
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColumnsAllColumns) {
  // Check table pattern and column pattern returns all columns
  this->connect();

  // Attempt to get all columns
  SQLWCHAR tablePattern[] = L"%";
  SQLWCHAR columnPattern[] = L"%";

  SQLRETURN ret = SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, tablePattern,
                             SQL_NTS, columnPattern, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColumns(this->stmt,
                  std::wstring(L"main"),          // expectedCatalog
                  std::wstring(L"foreignTable"),  // expectedTable
                  std::wstring(L"id"),            // expectedColumn
                  SQL_BIGINT,                     // expectedDataType
                  std::wstring(L"BIGINT"),        // expectedTypeName
                  10,            // expectedColumnSize (mock returns 10 instead of 19)
                  8,             // expectedBufferLength
                  15,            // expectedDecimalDigits (mock returns 15 instead of 0)
                  10,            // expectedNumPrecRadix
                  SQL_NULLABLE,  // expectedNullable
                  SQL_BIGINT,    // expectedSqlDataType
                  NULL,          // expectedDateTimeSub
                  8,             // expectedOctetCharLength
                  1,             // expectedOrdinalPosition
                  std::wstring(L"YES"));  // expectedIsNullable

  // Check 2nd Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColumns(this->stmt,
                  std::wstring(L"main"),          // expectedCatalog
                  std::wstring(L"foreignTable"),  // expectedTable
                  std::wstring(L"foreignName"),   // expectedColumn
                  SQL_WVARCHAR,                   // expectedDataType
                  std::wstring(L"WVARCHAR"),      // expectedTypeName
                  0,   // expectedColumnSize (mock server limitation: returns 0 for
                       // varchar(100), the ODBC spec expects 100)
                  0,   // expectedBufferLength
                  15,  // expectedDecimalDigits
                  0,   // expectedNumPrecRadix
                  SQL_NULLABLE,           // expectedNullable
                  SQL_WVARCHAR,           // expectedSqlDataType
                  NULL,                   // expectedDateTimeSub
                  0,                      // expectedOctetCharLength
                  2,                      // expectedOrdinalPosition
                  std::wstring(L"YES"));  // expectedIsNullable

  // Check 3rd Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColumns(this->stmt,
                  std::wstring(L"main"),          // expectedCatalog
                  std::wstring(L"foreignTable"),  // expectedTable
                  std::wstring(L"value"),         // expectedColumn
                  SQL_BIGINT,                     // expectedDataType
                  std::wstring(L"BIGINT"),        // expectedTypeName
                  10,            // expectedColumnSize (mock returns 10 instead of 19)
                  8,             // expectedBufferLength
                  15,            // expectedDecimalDigits (mock returns 15 instead of 0)
                  10,            // expectedNumPrecRadix
                  SQL_NULLABLE,  // expectedNullable
                  SQL_BIGINT,    // expectedSqlDataType
                  NULL,          // expectedDateTimeSub
                  8,             // expectedOctetCharLength
                  3,             // expectedOrdinalPosition
                  std::wstring(L"YES"));  // expectedIsNullable

  // Check 4th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColumns(this->stmt,
                  std::wstring(L"main"),      // expectedCatalog
                  std::wstring(L"intTable"),  // expectedTable
                  std::wstring(L"id"),        // expectedColumn
                  SQL_BIGINT,                 // expectedDataType
                  std::wstring(L"BIGINT"),    // expectedTypeName
                  10,            // expectedColumnSize (mock returns 10 instead of 19)
                  8,             // expectedBufferLength
                  15,            // expectedDecimalDigits (mock returns 15 instead of 0)
                  10,            // expectedNumPrecRadix
                  SQL_NULLABLE,  // expectedNullable
                  SQL_BIGINT,    // expectedSqlDataType
                  NULL,          // expectedDateTimeSub
                  8,             // expectedOctetCharLength
                  1,             // expectedOrdinalPosition
                  std::wstring(L"YES"));  // expectedIsNullable

  // Check 5th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColumns(this->stmt,
                  std::wstring(L"main"),      // expectedCatalog
                  std::wstring(L"intTable"),  // expectedTable
                  std::wstring(L"keyName"),   // expectedColumn
                  SQL_WVARCHAR,               // expectedDataType
                  std::wstring(L"WVARCHAR"),  // expectedTypeName
                  0,   // expectedColumnSize (mock server limitation: returns 0 for
                       // varchar(100), the ODBC spec expects 100)
                  0,   // expectedBufferLength
                  15,  // expectedDecimalDigits
                  0,   // expectedNumPrecRadix
                  SQL_NULLABLE,           // expectedNullable
                  SQL_WVARCHAR,           // expectedSqlDataType
                  NULL,                   // expectedDateTimeSub
                  0,                      // expectedOctetCharLength
                  2,                      // expectedOrdinalPosition
                  std::wstring(L"YES"));  // expectedIsNullable

  // Check 6th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColumns(this->stmt,
                  std::wstring(L"main"),      // expectedCatalog
                  std::wstring(L"intTable"),  // expectedTable
                  std::wstring(L"value"),     // expectedColumn
                  SQL_BIGINT,                 // expectedDataType
                  std::wstring(L"BIGINT"),    // expectedTypeName
                  10,            // expectedColumnSize (mock returns 10 instead of 19)
                  8,             // expectedBufferLength
                  15,            // expectedDecimalDigits (mock returns 15 instead of 0)
                  10,            // expectedNumPrecRadix
                  SQL_NULLABLE,  // expectedNullable
                  SQL_BIGINT,    // expectedSqlDataType
                  NULL,          // expectedDateTimeSub
                  8,             // expectedOctetCharLength
                  3,             // expectedOrdinalPosition
                  std::wstring(L"YES"));  // expectedIsNullable

  // Check 7th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColumns(this->stmt,
                  std::wstring(L"main"),       // expectedCatalog
                  std::wstring(L"intTable"),   // expectedTable
                  std::wstring(L"foreignId"),  // expectedColumn
                  SQL_BIGINT,                  // expectedDataType
                  std::wstring(L"BIGINT"),     // expectedTypeName
                  10,            // expectedColumnSize (mock returns 10 instead of 19)
                  8,             // expectedBufferLength
                  15,            // expectedDecimalDigits (mock returns 15 instead of 0)
                  10,            // expectedNumPrecRadix
                  SQL_NULLABLE,  // expectedNullable
                  SQL_BIGINT,    // expectedSqlDataType
                  NULL,          // expectedDateTimeSub
                  8,             // expectedOctetCharLength
                  4,             // expectedOrdinalPosition
                  std::wstring(L"YES"));  // expectedIsNullable

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColumnsAllTypes) {
  // Limitation: Mock server returns incorrect values for column size for some columns.
  // For character and binary type columns, the driver calculates buffer length and char
  // octet length from column size.

  // Checks filtering table with table name pattern
  this->connect();
  this->CreateTableAllDataType();

  // Attempt to get all columns from AllTypesTable
  SQLWCHAR tablePattern[] = L"AllTypesTable";
  SQLWCHAR columnPattern[] = L"%";

  SQLRETURN ret = SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, tablePattern,
                             SQL_NTS, columnPattern, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Fetch SQLColumn data for 1st column in AllTypesTable
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColumns(this->stmt,
                  std::wstring(L"main"),           // expectedCatalog
                  std::wstring(L"AllTypesTable"),  // expectedTable
                  std::wstring(L"bigint_col"),     // expectedColumn
                  SQL_BIGINT,                      // expectedDataType
                  std::wstring(L"BIGINT"),         // expectedTypeName
                  10,  // expectedColumnSize (mock server limitation: returns 10,
                       // the ODBC spec expects 19)
                  8,   // expectedBufferLength
                  15,  // expectedDecimalDigits (mock server limitation: returns 15,
                       // the ODBC spec expects 0)
                  10,  // expectedNumPrecRadix
                  SQL_NULLABLE,           // expectedNullable
                  SQL_BIGINT,             // expectedSqlDataType
                  NULL,                   // expectedDateTimeSub
                  8,                      // expectedOctetCharLength
                  1,                      // expectedOrdinalPosition
                  std::wstring(L"YES"));  // expectedIsNullable

  // Check SQLColumn data for 2nd column in AllTypesTable
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColumns(this->stmt,
                  std::wstring(L"main"),           // expectedCatalog
                  std::wstring(L"AllTypesTable"),  // expectedTable
                  std::wstring(L"char_col"),       // expectedColumn
                  SQL_WVARCHAR,                    // expectedDataType
                  std::wstring(L"WVARCHAR"),       // expectedTypeName
                  0,   // expectedColumnSize (mock server limitation: returns 0 for
                       // varchar(100), the ODBC spec expects 100)
                  0,   // expectedBufferLength
                  15,  // expectedDecimalDigits
                  0,   // expectedNumPrecRadix
                  SQL_NULLABLE,           // expectedNullable
                  SQL_WVARCHAR,           // expectedSqlDataType
                  NULL,                   // expectedDateTimeSub
                  0,                      // expectedOctetCharLength
                  2,                      // expectedOrdinalPosition
                  std::wstring(L"YES"));  // expectedIsNullable

  // Check SQLColumn data for 3rd column in AllTypesTable
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColumns(this->stmt,
                  std::wstring(L"main"),           // expectedCatalog
                  std::wstring(L"AllTypesTable"),  // expectedTable
                  std::wstring(L"varbinary_col"),  // expectedColumn
                  SQL_BINARY,                      // expectedDataType
                  std::wstring(L"BINARY"),         // expectedTypeName
                  0,   // expectedColumnSize (mock server limitation: returns 0 for BLOB
                       // column, spec expects binary data limit)
                  0,   // expectedBufferLength
                  15,  // expectedDecimalDigits
                  0,   // expectedNumPrecRadix
                  SQL_NULLABLE,           // expectedNullable
                  SQL_BINARY,             // expectedSqlDataType
                  NULL,                   // expectedDateTimeSub
                  0,                      // expectedOctetCharLength
                  3,                      // expectedOrdinalPosition
                  std::wstring(L"YES"));  // expectedIsNullable

  // Check SQLColumn data for 4th column in AllTypesTable
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColumns(this->stmt,
                  std::wstring(L"main"),           // expectedCatalog
                  std::wstring(L"AllTypesTable"),  // expectedTable
                  std::wstring(L"double_col"),     // expectedColumn
                  SQL_DOUBLE,                      // expectedDataType
                  std::wstring(L"DOUBLE"),         // expectedTypeName
                  15,                              // expectedColumnSize
                  8,                               // expectedBufferLength
                  15,                              // expectedDecimalDigits
                  2,                               // expectedNumPrecRadix
                  SQL_NULLABLE,                    // expectedNullable
                  SQL_DOUBLE,                      // expectedSqlDataType
                  NULL,                            // expectedDateTimeSub
                  8,                               // expectedOctetCharLength
                  4,                               // expectedOrdinalPosition
                  std::wstring(L"YES"));           // expectedIsNullable

  // There should be no more column data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColumnsUnicode) {
  // Limitation: Mock server returns incorrect values for column size for some columns.
  // For character and binary type columns, the driver calculates buffer length and char
  // octet length from column size.
  this->connect();
  this->CreateUnicodeTable();

  // Attempt to get all columns
  SQLWCHAR tablePattern[] = L"数据";
  SQLWCHAR columnPattern[] = L"%";

  SQLRETURN ret = SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, tablePattern,
                             SQL_NTS, columnPattern, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check SQLColumn data for 1st column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColumns(this->stmt,
                  std::wstring(L"main"),      // expectedCatalog
                  std::wstring(L"数据"),      // expectedTable
                  std::wstring(L"资料"),      // expectedColumn
                  SQL_WVARCHAR,               // expectedDataType
                  std::wstring(L"WVARCHAR"),  // expectedTypeName
                  0,   // expectedColumnSize (mock server limitation: returns 0 for
                       // varchar(100), spec expects 100)
                  0,   // expectedBufferLength
                  15,  // expectedDecimalDigits
                  0,   // expectedNumPrecRadix
                  SQL_NULLABLE,           // expectedNullable
                  SQL_WVARCHAR,           // expectedSqlDataType
                  NULL,                   // expectedDateTimeSub
                  0,                      // expectedOctetCharLength
                  1,                      // expectedOrdinalPosition
                  std::wstring(L"YES"));  // expectedIsNullable

  // There should be no more column data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColumnsColumnPattern) {
  // Checks filtering table with column name pattern.
  // Only check table and column name
  this->connect();

  SQLWCHAR tablePattern[] = L"%";
  SQLWCHAR columnPattern[] = L"id";

  SQLRETURN ret = SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, tablePattern,
                             SQL_NTS, columnPattern, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check 1st Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColumns(this->stmt,
                  std::wstring(L"main"),          // expectedCatalog
                  std::wstring(L"foreignTable"),  // expectedTable
                  std::wstring(L"id"),            // expectedColumn
                  SQL_BIGINT,                     // expectedDataType
                  std::wstring(L"BIGINT"),        // expectedTypeName
                  10,            // expectedColumnSize (mock returns 10 instead of 19)
                  8,             // expectedBufferLength
                  15,            // expectedDecimalDigits (mock returns 15 instead of 0)
                  10,            // expectedNumPrecRadix
                  SQL_NULLABLE,  // expectedNullable
                  SQL_BIGINT,    // expectedSqlDataType
                  NULL,          // expectedDateTimeSub
                  8,             // expectedOctetCharLength
                  1,             // expectedOrdinalPosition
                  std::wstring(L"YES"));  // expectedIsNullable

  // Check 2nd Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColumns(this->stmt,
                  std::wstring(L"main"),      // expectedCatalog
                  std::wstring(L"intTable"),  // expectedTable
                  std::wstring(L"id"),        // expectedColumn
                  SQL_BIGINT,                 // expectedDataType
                  std::wstring(L"BIGINT"),    // expectedTypeName
                  10,            // expectedColumnSize (mock returns 10 instead of 19)
                  8,             // expectedBufferLength
                  15,            // expectedDecimalDigits (mock returns 15 instead of 0)
                  10,            // expectedNumPrecRadix
                  SQL_NULLABLE,  // expectedNullable
                  SQL_BIGINT,    // expectedSqlDataType
                  NULL,          // expectedDateTimeSub
                  8,             // expectedOctetCharLength
                  1,             // expectedOrdinalPosition
                  std::wstring(L"YES"));  // expectedIsNullable

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColumnsTableColumnPattern) {
  // Checks filtering table with table and column name pattern.
  // Only check table and column name
  this->connect();

  SQLWCHAR tablePattern[] = L"foreignTable";
  SQLWCHAR columnPattern[] = L"id";

  SQLRETURN ret = SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, tablePattern,
                             SQL_NTS, columnPattern, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check 1st Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColumns(this->stmt,
                  std::wstring(L"main"),          // expectedCatalog
                  std::wstring(L"foreignTable"),  // expectedTable
                  std::wstring(L"id"),            // expectedColumn
                  SQL_BIGINT,                     // expectedDataType
                  std::wstring(L"BIGINT"),        // expectedTypeName
                  10,            // expectedColumnSize (mock returns 10 instead of 19)
                  8,             // expectedBufferLength
                  15,            // expectedDecimalDigits (mock returns 15 instead of 0)
                  10,            // expectedNumPrecRadix
                  SQL_NULLABLE,  // expectedNullable
                  SQL_BIGINT,    // expectedSqlDataType
                  NULL,          // expectedDateTimeSub
                  8,             // expectedOctetCharLength
                  1,             // expectedOrdinalPosition
                  std::wstring(L"YES"));  // expectedIsNullable

  // There is no more column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColumnsInvalidTablePattern) {
  this->connect();

  SQLWCHAR tablePattern[] = L"non-existent-table";
  SQLWCHAR columnPattern[] = L"%";

  SQLRETURN ret = SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, tablePattern,
                             SQL_NTS, columnPattern, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // There is no column from filter
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

}  // namespace arrow::flight::sql::odbc
