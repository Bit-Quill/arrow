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

// TODO: add tests with SQLDescribeCol to check metadata of SQLColumns for ODBC 2 and
// ODBC 3.

namespace arrow::flight::sql::odbc {
// Helper functions
void checkSQLColumns(
    SQLHSTMT stmt, const std::wstring& expectedTable, const std::wstring& expectedColumn,
    const SQLINTEGER& expectedDataType, const std::wstring& expectedTypeName,
    const SQLINTEGER& expectedColumnSize, const SQLINTEGER& expectedBufferLength,
    const SQLSMALLINT& expectedDecimalDigits, const SQLSMALLINT& expectedNumPrecRadix,
    const SQLSMALLINT& expectedNullable, const SQLSMALLINT& expectedSqlDataType,
    const SQLSMALLINT& expectedDateTimeSub, const SQLINTEGER& expectedOctetCharLength,
    const SQLINTEGER& expectedOrdinalPosition, const std::wstring& expectedIsNullable) {
  CheckStringColumnW(stmt, 3, expectedTable);   // table name
  CheckStringColumnW(stmt, 4, expectedColumn);  // column name

  CheckIntColumn(stmt, 5, expectedDataType);  // data type

  CheckStringColumnW(stmt, 6, expectedTypeName);  // type name

  CheckIntColumn(stmt, 7, expectedColumnSize);    // column size
  CheckIntColumn(stmt, 8, expectedBufferLength);  // buffer length

  CheckSmallIntColumn(stmt, 9, expectedDecimalDigits);  // decimal digits
  CheckSmallIntColumn(stmt, 10, expectedNumPrecRadix);  // num prec radix
  CheckSmallIntColumn(stmt, 11,
                      expectedNullable);  // nullable

  CheckNullColumnW(stmt, 12);  // remarks
  CheckNullColumnW(stmt, 13);  // column def

  CheckSmallIntColumn(stmt, 14, expectedSqlDataType);  // sql data type
  CheckSmallIntColumn(stmt, 15, expectedDateTimeSub);  // sql date type sub
  CheckIntColumn(stmt, 16, expectedOctetCharLength);   // char octet length
  CheckIntColumn(stmt, 17,
                 expectedOrdinalPosition);  // oridinal position

  CheckStringColumnW(stmt, 18, expectedIsNullable);  // is nullable
}

void checkMockSQLColumns(
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

  checkSQLColumns(stmt, expectedTable, expectedColumn, expectedDataType, expectedTypeName,
                  expectedColumnSize, expectedBufferLength, expectedDecimalDigits,
                  expectedNumPrecRadix, expectedNullable, expectedSqlDataType,
                  expectedDateTimeSub, expectedOctetCharLength, expectedOrdinalPosition,
                  expectedIsNullable);
}

void checkRemoteSQLColumns(
    SQLHSTMT stmt, const std::wstring& expectedSchema, const std::wstring& expectedTable,
    const std::wstring& expectedColumn, const SQLINTEGER& expectedDataType,
    const std::wstring& expectedTypeName, const SQLINTEGER& expectedColumnSize,
    const SQLINTEGER& expectedBufferLength, const SQLSMALLINT& expectedDecimalDigits,
    const SQLSMALLINT& expectedNumPrecRadix, const SQLSMALLINT& expectedNullable,
    const SQLSMALLINT& expectedSqlDataType, const SQLSMALLINT& expectedDateTimeSub,
    const SQLINTEGER& expectedOctetCharLength, const SQLINTEGER& expectedOrdinalPosition,
    const std::wstring& expectedIsNullable) {
  CheckNullColumnW(stmt, 1);                    // catalog
  CheckStringColumnW(stmt, 2, expectedSchema);  // schema
  checkSQLColumns(stmt, expectedTable, expectedColumn, expectedDataType, expectedTypeName,
                  expectedColumnSize, expectedBufferLength, expectedDecimalDigits,
                  expectedNumPrecRadix, expectedNullable, expectedSqlDataType,
                  expectedDateTimeSub, expectedOctetCharLength, expectedOrdinalPosition,
                  expectedIsNullable);
}

void checkSQLColAttribute(SQLHSTMT stmt, SQLUSMALLINT idx,
                          const std::wstring& expectedColmnName, SQLLEN expectedDataType,
                          SQLLEN expectedConciseType, SQLLEN expectedDisplaySize,
                          SQLLEN expectedPrecScale, SQLLEN expectedLength,
                          const std::wstring& expectedLiteralPrefix,
                          const std::wstring& expectedLiteralSuffix,
                          SQLLEN expectedColumnSize, SQLLEN expectedColumnScale,
                          SQLLEN expectedColumnNullability, SQLLEN expectedNumPrecRadix,
                          SQLLEN expectedOctetLength, SQLLEN expectedSearchable,
                          SQLLEN expectedUnsignedColumn) {
  std::vector<SQLWCHAR> name(ODBC_BUFFER_SIZE);
  SQLSMALLINT nameLen = 0;
  std::vector<SQLWCHAR> baseColumnName(ODBC_BUFFER_SIZE);
  SQLSMALLINT columnNameLen = 0;
  std::vector<SQLWCHAR> label(ODBC_BUFFER_SIZE);
  SQLSMALLINT labelLen = 0;
  std::vector<SQLWCHAR> prefix(ODBC_BUFFER_SIZE);
  SQLSMALLINT prefixLen = 0;
  std::vector<SQLWCHAR> suffix(ODBC_BUFFER_SIZE);
  SQLSMALLINT suffixLen = 0;
  SQLLEN dataType = 0;
  SQLLEN conciseType = 0;
  SQLLEN displaySize = 0;
  SQLLEN precScale = 0;
  SQLLEN length = 0;
  SQLLEN size = 0;
  SQLLEN scale = 0;
  SQLLEN nullability = 0;
  SQLLEN numPrecRadix = 0;
  SQLLEN octetLength = 0;
  SQLLEN searchable = 0;
  SQLLEN unsignedCol = 0;

  SQLRETURN ret = SQLColAttribute(stmt, idx, SQL_DESC_NAME, &name[0],
                                  (SQLSMALLINT)name.size(), &nameLen, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_BASE_COLUMN_NAME, &baseColumnName[0],
                        (SQLSMALLINT)baseColumnName.size(), &columnNameLen, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_LABEL, &label[0], (SQLSMALLINT)label.size(),
                        &labelLen, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_TYPE, 0, 0, 0, &dataType);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_CONCISE_TYPE, 0, 0, 0, &conciseType);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_DISPLAY_SIZE, 0, 0, 0, &displaySize);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_FIXED_PREC_SCALE, 0, 0, 0, &precScale);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_LENGTH, 0, 0, 0, &length);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_LITERAL_PREFIX, &prefix[0],
                        (SQLSMALLINT)prefix.size(), &prefixLen, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_LITERAL_SUFFIX, &suffix[0],
                        (SQLSMALLINT)suffix.size(), &suffixLen, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_PRECISION, 0, 0, 0, &size);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_SCALE, 0, 0, 0, &scale);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_NULLABLE, 0, 0, 0, &nullability);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_NUM_PREC_RADIX, 0, 0, 0, &numPrecRadix);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_OCTET_LENGTH, 0, 0, 0, &octetLength);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_SEARCHABLE, 0, 0, 0, &searchable);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_UNSIGNED, 0, 0, 0, &unsignedCol);
  EXPECT_EQ(ret, SQL_SUCCESS);

  std::wstring nameStr = ConvertToWString(name, nameLen);
  std::wstring baseColumnNameStr = ConvertToWString(baseColumnName, columnNameLen);
  std::wstring labelStr = ConvertToWString(label, labelLen);
  std::wstring prefixStr = ConvertToWString(prefix, prefixLen);

  // Assume column name, base column name, and label are equivalent in the result set
  EXPECT_EQ(nameStr, expectedColmnName);
  EXPECT_EQ(baseColumnNameStr, expectedColmnName);
  EXPECT_EQ(labelStr, expectedColmnName);
  EXPECT_EQ(dataType, expectedDataType);
  EXPECT_EQ(conciseType, expectedConciseType);
  EXPECT_EQ(displaySize, expectedDisplaySize);
  EXPECT_EQ(precScale, expectedPrecScale);
  EXPECT_EQ(length, expectedLength);
  EXPECT_EQ(prefixStr, expectedLiteralPrefix);
  EXPECT_EQ(size, expectedColumnSize);
  EXPECT_EQ(scale, expectedColumnScale);
  EXPECT_EQ(nullability, expectedColumnNullability);
  EXPECT_EQ(numPrecRadix, expectedNumPrecRadix);
  EXPECT_EQ(octetLength, expectedOctetLength);
  EXPECT_EQ(searchable, expectedSearchable);
  EXPECT_EQ(unsignedCol, expectedUnsignedColumn);
}

void checkSQLColAttributeString(SQLHSTMT stmt, const std::wstring& wsql, SQLUSMALLINT idx,
                                SQLUSMALLINT fieldIdentifier,
                                const std::wstring& expectedAttrString) {
  // Execute query and check SQLColAttribute string attribute
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());
  SQLRETURN ret = SQLExecDirect(stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  std::vector<SQLWCHAR> strVal(ODBC_BUFFER_SIZE);
  SQLSMALLINT strLen = 0;

  ret = SQLColAttribute(stmt, idx, fieldIdentifier, &strVal[0],
                        (SQLSMALLINT)strVal.size(), &strLen, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  std::wstring attrStr = ConvertToWString(strVal, strLen);
  EXPECT_EQ(attrStr, expectedAttrString);
}

void checkSQLColAttributeNumeric(SQLHSTMT stmt, const std::wstring& wsql,
                                 SQLUSMALLINT idx, SQLUSMALLINT fieldIdentifier,
                                 SQLLEN expectedAttrNumeric) {
  // Execute query and check SQLColAttribute numeric attribute
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());
  SQLRETURN ret = SQLExecDirect(stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLLEN numVal = 0;
  ret = SQLColAttribute(stmt, idx, fieldIdentifier, 0, 0, 0, &numVal);
  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(numVal, expectedAttrNumeric);
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

  // mock limitation: SQLite mock server returns 10 for bigint size when spec indicates
  // should be 19
  // DECIMAL_DIGITS should be 0 for bigint type since it is exact
  // mock limitation: SQLite mock server returns 10 for bigint decimal digits when spec
  // indicates should be 0
  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),          // expectedCatalog
                      std::wstring(L"foreignTable"),  // expectedTable
                      std::wstring(L"id"),            // expectedColumn
                      SQL_BIGINT,                     // expectedDataType
                      std::wstring(L"BIGINT"),        // expectedTypeName
                      10,  // expectedColumnSize (mock returns 10 instead of 19)
                      8,   // expectedBufferLength
                      15,  // expectedDecimalDigits (mock returns 15 instead of 0)
                      10,  // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_BIGINT,             // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      8,                      // expectedOctetCharLength
                      1,                      // expectedOrdinalPosition
                      std::wstring(L"YES"));  // expectedIsNullable

  // Check 2nd Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkMockSQLColumns(this->stmt,
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

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),          // expectedCatalog
                      std::wstring(L"foreignTable"),  // expectedTable
                      std::wstring(L"value"),         // expectedColumn
                      SQL_BIGINT,                     // expectedDataType
                      std::wstring(L"BIGINT"),        // expectedTypeName
                      10,  // expectedColumnSize (mock returns 10 instead of 19)
                      8,   // expectedBufferLength
                      15,  // expectedDecimalDigits (mock returns 15 instead of 0)
                      10,  // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_BIGINT,             // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      8,                      // expectedOctetCharLength
                      3,                      // expectedOrdinalPosition
                      std::wstring(L"YES"));  // expectedIsNullable

  // Check 4th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),      // expectedCatalog
                      std::wstring(L"intTable"),  // expectedTable
                      std::wstring(L"id"),        // expectedColumn
                      SQL_BIGINT,                 // expectedDataType
                      std::wstring(L"BIGINT"),    // expectedTypeName
                      10,  // expectedColumnSize (mock returns 10 instead of 19)
                      8,   // expectedBufferLength
                      15,  // expectedDecimalDigits (mock returns 15 instead of 0)
                      10,  // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_BIGINT,             // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      8,                      // expectedOctetCharLength
                      1,                      // expectedOrdinalPosition
                      std::wstring(L"YES"));  // expectedIsNullable

  // Check 5th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkMockSQLColumns(this->stmt,
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

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),      // expectedCatalog
                      std::wstring(L"intTable"),  // expectedTable
                      std::wstring(L"value"),     // expectedColumn
                      SQL_BIGINT,                 // expectedDataType
                      std::wstring(L"BIGINT"),    // expectedTypeName
                      10,  // expectedColumnSize (mock returns 10 instead of 19)
                      8,   // expectedBufferLength
                      15,  // expectedDecimalDigits (mock returns 15 instead of 0)
                      10,  // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_BIGINT,             // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      8,                      // expectedOctetCharLength
                      3,                      // expectedOrdinalPosition
                      std::wstring(L"YES"));  // expectedIsNullable

  // Check 7th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),       // expectedCatalog
                      std::wstring(L"intTable"),   // expectedTable
                      std::wstring(L"foreignId"),  // expectedColumn
                      SQL_BIGINT,                  // expectedDataType
                      std::wstring(L"BIGINT"),     // expectedTypeName
                      10,  // expectedColumnSize (mock returns 10 instead of 19)
                      8,   // expectedBufferLength
                      15,  // expectedDecimalDigits (mock returns 15 instead of 0)
                      10,  // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_BIGINT,             // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      8,                      // expectedOctetCharLength
                      4,                      // expectedOrdinalPosition
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

  checkMockSQLColumns(this->stmt,
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

  checkMockSQLColumns(this->stmt,
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

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),           // expectedCatalog
                      std::wstring(L"AllTypesTable"),  // expectedTable
                      std::wstring(L"varbinary_col"),  // expectedColumn
                      SQL_BINARY,                      // expectedDataType
                      std::wstring(L"BINARY"),         // expectedTypeName
                      0,   // expectedColumnSize (mock server limitation: returns 0 for
                           // BLOB column, spec expects binary data limit)
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

  checkMockSQLColumns(this->stmt,
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

  checkMockSQLColumns(this->stmt,
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

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLColumnsAllTypes) {
  // GH-47159: Return NUM_PREC_RADIX based on whether COLUMN_SIZE contains number of
  // digits or bits
  this->connect();

  SQLWCHAR tablePattern[] = L"ODBCTest";
  SQLWCHAR columnPattern[] = L"%";

  SQLRETURN ret = SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, tablePattern,
                             SQL_NTS, columnPattern, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check 1st Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),      // expectedSchema
                        std::wstring(L"ODBCTest"),      // expectedTable
                        std::wstring(L"sinteger_max"),  // expectedColumn
                        SQL_INTEGER,                    // expectedDataType
                        std::wstring(L"INTEGER"),       // expectedTypeName
                        32,  // expectedColumnSize (remote server returns number of bits)
                        4,   // expectedBufferLength
                        0,   // expectedDecimalDigits
                        10,  // expectedNumPrecRadix
                        SQL_NULLABLE,           // expectedNullable
                        SQL_INTEGER,            // expectedSqlDataType
                        NULL,                   // expectedDateTimeSub
                        4,                      // expectedOctetCharLength
                        1,                      // expectedOrdinalPosition
                        std::wstring(L"YES"));  // expectedIsNullable

  // Check 2nd Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),     // expectedSchema
                        std::wstring(L"ODBCTest"),     // expectedTable
                        std::wstring(L"sbigint_max"),  // expectedColumn
                        SQL_BIGINT,                    // expectedDataType
                        std::wstring(L"BIGINT"),       // expectedTypeName
                        64,  // expectedColumnSize (remote server returns number of bits)
                        8,   // expectedBufferLength
                        0,   // expectedDecimalDigits
                        10,  // expectedNumPrecRadix
                        SQL_NULLABLE,           // expectedNullable
                        SQL_BIGINT,             // expectedSqlDataType
                        NULL,                   // expectedDateTimeSub
                        8,                      // expectedOctetCharLength
                        2,                      // expectedOrdinalPosition
                        std::wstring(L"YES"));  // expectedIsNullable

  // Check 3rd Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),          // expectedSchema
                        std::wstring(L"ODBCTest"),          // expectedTable
                        std::wstring(L"decimal_positive"),  // expectedColumn
                        SQL_DECIMAL,                        // expectedDataType
                        std::wstring(L"DECIMAL"),           // expectedTypeName
                        38,                                 // expectedColumnSize
                        19,                                 // expectedBufferLength
                        0,                                  // expectedDecimalDigits
                        10,                                 // expectedNumPrecRadix
                        SQL_NULLABLE,                       // expectedNullable
                        SQL_DECIMAL,                        // expectedSqlDataType
                        NULL,                               // expectedDateTimeSub
                        2,                                  // expectedOctetCharLength
                        3,                                  // expectedOrdinalPosition
                        std::wstring(L"YES"));              // expectedIsNullable

  // Check 4th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),   // expectedSchema
                        std::wstring(L"ODBCTest"),   // expectedTable
                        std::wstring(L"float_max"),  // expectedColumn
                        SQL_FLOAT,                   // expectedDataType
                        std::wstring(L"FLOAT"),      // expectedTypeName
                        24,  // expectedColumnSize (precision bits from IEEE 754)
                        8,   // expectedBufferLength
                        0,   // expectedDecimalDigits
                        2,   // expectedNumPrecRadix
                        SQL_NULLABLE,           // expectedNullable
                        SQL_FLOAT,              // expectedSqlDataType
                        NULL,                   // expectedDateTimeSub
                        8,                      // expectedOctetCharLength
                        4,                      // expectedOrdinalPosition
                        std::wstring(L"YES"));  // expectedIsNullable

  // Check 5th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),    // expectedSchema
                        std::wstring(L"ODBCTest"),    // expectedTable
                        std::wstring(L"double_max"),  // expectedColumn
                        SQL_DOUBLE,                   // expectedDataType
                        std::wstring(L"DOUBLE"),      // expectedTypeName
                        53,  // expectedColumnSize (precision bits from IEEE 754)
                        8,   // expectedBufferLength
                        0,   // expectedDecimalDigits
                        2,   // expectedNumPrecRadix
                        SQL_NULLABLE,           // expectedNullable
                        SQL_DOUBLE,             // expectedSqlDataType
                        NULL,                   // expectedDateTimeSub
                        8,                      // expectedOctetCharLength
                        5,                      // expectedOrdinalPosition
                        std::wstring(L"YES"));  // expectedIsNullable

  // Check 6th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),  // expectedSchema
                        std::wstring(L"ODBCTest"),  // expectedTable
                        std::wstring(L"bit_true"),  // expectedColumn
                        SQL_BIT,                    // expectedDataType
                        std::wstring(L"BOOLEAN"),   // expectedTypeName
                        0,  // expectedColumnSize (limitation: remote server remote server
                            // returns 0, should be 1)
                        1,  // expectedBufferLength
                        0,  // expectedDecimalDigits
                        0,  // expectedNumPrecRadix
                        SQL_NULLABLE,           // expectedNullable
                        SQL_BIT,                // expectedSqlDataType
                        NULL,                   // expectedDateTimeSub
                        1,                      // expectedOctetCharLength
                        6,                      // expectedOrdinalPosition
                        std::wstring(L"YES"));  // expectedIsNullable

  // ODBC ver 3 returns SQL_TYPE_DATE, SQL_TYPE_TIME, and SQL_TYPE_TIMESTAMP in the
  // DATA_TYPE field

  // Check 7th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),  // expectedSchema
      std::wstring(L"ODBCTest"),  // expectedTable
      std::wstring(L"date_max"),  // expectedColumn
      SQL_TYPE_DATE,              // expectedDataType
      std::wstring(L"DATE"),      // expectedTypeName
      0,   // expectedColumnSize (limitation: remote server returns 0, should be 10)
      10,  // expectedBufferLength
      0,   // expectedDecimalDigits
      0,   // expectedNumPrecRadix
      SQL_NULLABLE,           // expectedNullable
      SQL_DATETIME,           // expectedSqlDataType
      SQL_CODE_DATE,          // expectedDateTimeSub
      6,                      // expectedOctetCharLength
      7,                      // expectedOrdinalPosition
      std::wstring(L"YES"));  // expectedIsNullable

  // Check 8th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),  // expectedSchema
      std::wstring(L"ODBCTest"),  // expectedTable
      std::wstring(L"time_max"),  // expectedColumn
      SQL_TYPE_TIME,              // expectedDataType
      std::wstring(L"TIME"),      // expectedTypeName
      3,              // expectedColumnSize (limitation: should be 9+fractional digits)
      12,             // expectedBufferLength
      0,              // expectedDecimalDigits
      0,              // expectedNumPrecRadix
      SQL_NULLABLE,   // expectedNullable
      SQL_DATETIME,   // expectedSqlDataType
      SQL_CODE_TIME,  // expectedDateTimeSub
      6,              // expectedOctetCharLength
      8,              // expectedOrdinalPosition
      std::wstring(L"YES"));  // expectedIsNullable

  // Check 9th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),       // expectedSchema
      std::wstring(L"ODBCTest"),       // expectedTable
      std::wstring(L"timestamp_max"),  // expectedColumn
      SQL_TYPE_TIMESTAMP,              // expectedDataType
      std::wstring(L"TIMESTAMP"),      // expectedTypeName
      3,             // expectedColumnSize (limitation: should be 20+fractional digits)
      23,            // expectedBufferLength
      0,             // expectedDecimalDigits
      0,             // expectedNumPrecRadix
      SQL_NULLABLE,  // expectedNullable
      SQL_DATETIME,  // expectedSqlDataType
      SQL_CODE_TIMESTAMP,     // expectedDateTimeSub
      16,                     // expectedOctetCharLength
      9,                      // expectedOrdinalPosition
      std::wstring(L"YES"));  // expectedIsNullable

  // There is no more column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLColumnsAllTypesODBCVer2) {
  // GH-47159: Return NUM_PREC_RADIX based on whether COLUMN_SIZE contains number of
  // digits or bits
  this->connect(SQL_OV_ODBC2);

  SQLWCHAR tablePattern[] = L"ODBCTest";
  SQLWCHAR columnPattern[] = L"%";

  SQLRETURN ret = SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, tablePattern,
                             SQL_NTS, columnPattern, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check 1st Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),      // expectedSchema
                        std::wstring(L"ODBCTest"),      // expectedTable
                        std::wstring(L"sinteger_max"),  // expectedColumn
                        SQL_INTEGER,                    // expectedDataType
                        std::wstring(L"INTEGER"),       // expectedTypeName
                        32,  // expectedColumnSize (remote server returns number of bits)
                        4,   // expectedBufferLength
                        0,   // expectedDecimalDigits
                        10,  // expectedNumPrecRadix
                        SQL_NULLABLE,           // expectedNullable
                        SQL_INTEGER,            // expectedSqlDataType
                        NULL,                   // expectedDateTimeSub
                        4,                      // expectedOctetCharLength
                        1,                      // expectedOrdinalPosition
                        std::wstring(L"YES"));  // expectedIsNullable

  // Check 2nd Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),     // expectedSchema
                        std::wstring(L"ODBCTest"),     // expectedTable
                        std::wstring(L"sbigint_max"),  // expectedColumn
                        SQL_BIGINT,                    // expectedDataType
                        std::wstring(L"BIGINT"),       // expectedTypeName
                        64,  // expectedColumnSize (remote server returns number of bits)
                        8,   // expectedBufferLength
                        0,   // expectedDecimalDigits
                        10,  // expectedNumPrecRadix
                        SQL_NULLABLE,           // expectedNullable
                        SQL_BIGINT,             // expectedSqlDataType
                        NULL,                   // expectedDateTimeSub
                        8,                      // expectedOctetCharLength
                        2,                      // expectedOrdinalPosition
                        std::wstring(L"YES"));  // expectedIsNullable

  // Check 3rd Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),          // expectedSchema
                        std::wstring(L"ODBCTest"),          // expectedTable
                        std::wstring(L"decimal_positive"),  // expectedColumn
                        SQL_DECIMAL,                        // expectedDataType
                        std::wstring(L"DECIMAL"),           // expectedTypeName
                        38,                                 // expectedColumnSize
                        19,                                 // expectedBufferLength
                        0,                                  // expectedDecimalDigits
                        10,                                 // expectedNumPrecRadix
                        SQL_NULLABLE,                       // expectedNullable
                        SQL_DECIMAL,                        // expectedSqlDataType
                        NULL,                               // expectedDateTimeSub
                        2,                                  // expectedOctetCharLength
                        3,                                  // expectedOrdinalPosition
                        std::wstring(L"YES"));              // expectedIsNullable

  // Check 4th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),   // expectedSchema
                        std::wstring(L"ODBCTest"),   // expectedTable
                        std::wstring(L"float_max"),  // expectedColumn
                        SQL_FLOAT,                   // expectedDataType
                        std::wstring(L"FLOAT"),      // expectedTypeName
                        24,  // expectedColumnSize (precision bits from IEEE 754)
                        8,   // expectedBufferLength
                        0,   // expectedDecimalDigits
                        2,   // expectedNumPrecRadix
                        SQL_NULLABLE,           // expectedNullable
                        SQL_FLOAT,              // expectedSqlDataType
                        NULL,                   // expectedDateTimeSub
                        8,                      // expectedOctetCharLength
                        4,                      // expectedOrdinalPosition
                        std::wstring(L"YES"));  // expectedIsNullable

  // Check 5th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),    // expectedSchema
                        std::wstring(L"ODBCTest"),    // expectedTable
                        std::wstring(L"double_max"),  // expectedColumn
                        SQL_DOUBLE,                   // expectedDataType
                        std::wstring(L"DOUBLE"),      // expectedTypeName
                        53,  // expectedColumnSize (precision bits from IEEE 754)
                        8,   // expectedBufferLength
                        0,   // expectedDecimalDigits
                        2,   // expectedNumPrecRadix
                        SQL_NULLABLE,           // expectedNullable
                        SQL_DOUBLE,             // expectedSqlDataType
                        NULL,                   // expectedDateTimeSub
                        8,                      // expectedOctetCharLength
                        5,                      // expectedOrdinalPosition
                        std::wstring(L"YES"));  // expectedIsNullable

  // Check 6th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),  // expectedSchema
                        std::wstring(L"ODBCTest"),  // expectedTable
                        std::wstring(L"bit_true"),  // expectedColumn
                        SQL_BIT,                    // expectedDataType
                        std::wstring(L"BOOLEAN"),   // expectedTypeName
                        0,  // expectedColumnSize (limitation: remote server remote server
                            // returns 0, should be 1)
                        1,  // expectedBufferLength
                        0,  // expectedDecimalDigits
                        0,  // expectedNumPrecRadix
                        SQL_NULLABLE,           // expectedNullable
                        SQL_BIT,                // expectedSqlDataType
                        NULL,                   // expectedDateTimeSub
                        1,                      // expectedOctetCharLength
                        6,                      // expectedOrdinalPosition
                        std::wstring(L"YES"));  // expectedIsNullable

  // ODBC ver 2 returns SQL_DATE, SQL_TIME, and SQL_TIMESTAMP in the DATA_TYPE field

  // Check 7th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),  // expectedSchema
      std::wstring(L"ODBCTest"),  // expectedTable
      std::wstring(L"date_max"),  // expectedColumn
      SQL_DATE,                   // expectedDataType
      std::wstring(L"DATE"),      // expectedTypeName
      0,   // expectedColumnSize (limitation: remote server returns 0, should be 10)
      10,  // expectedBufferLength
      0,   // expectedDecimalDigits
      0,   // expectedNumPrecRadix
      SQL_NULLABLE,           // expectedNullable
      SQL_DATETIME,           // expectedSqlDataType
      SQL_CODE_DATE,          // expectedDateTimeSub
      6,                      // expectedOctetCharLength
      7,                      // expectedOrdinalPosition
      std::wstring(L"YES"));  // expectedIsNullable

  // Check 8th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),  // expectedSchema
      std::wstring(L"ODBCTest"),  // expectedTable
      std::wstring(L"time_max"),  // expectedColumn
      SQL_TIME,                   // expectedDataType
      std::wstring(L"TIME"),      // expectedTypeName
      3,              // expectedColumnSize (limitation: should be 9+fractional digits)
      12,             // expectedBufferLength
      0,              // expectedDecimalDigits
      0,              // expectedNumPrecRadix
      SQL_NULLABLE,   // expectedNullable
      SQL_DATETIME,   // expectedSqlDataType
      SQL_CODE_TIME,  // expectedDateTimeSub
      6,              // expectedOctetCharLength
      8,              // expectedOrdinalPosition
      std::wstring(L"YES"));  // expectedIsNullable

  // Check 9th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),       // expectedSchema
      std::wstring(L"ODBCTest"),       // expectedTable
      std::wstring(L"timestamp_max"),  // expectedColumn
      SQL_TIMESTAMP,                   // expectedDataType
      std::wstring(L"TIMESTAMP"),      // expectedTypeName
      3,             // expectedColumnSize (limitation: should be 20+fractional digits)
      23,            // expectedBufferLength
      0,             // expectedDecimalDigits
      0,             // expectedNumPrecRadix
      SQL_NULLABLE,  // expectedNullable
      SQL_DATETIME,  // expectedSqlDataType
      SQL_CODE_TIMESTAMP,     // expectedDateTimeSub
      16,                     // expectedOctetCharLength
      9,                      // expectedOrdinalPosition
      std::wstring(L"YES"));  // expectedIsNullable

  // There is no more column
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

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),          // expectedCatalog
                      std::wstring(L"foreignTable"),  // expectedTable
                      std::wstring(L"id"),            // expectedColumn
                      SQL_BIGINT,                     // expectedDataType
                      std::wstring(L"BIGINT"),        // expectedTypeName
                      10,  // expectedColumnSize (mock returns 10 instead of 19)
                      8,   // expectedBufferLength
                      15,  // expectedDecimalDigits (mock returns 15 instead of 0)
                      10,  // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_BIGINT,             // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      8,                      // expectedOctetCharLength
                      1,                      // expectedOrdinalPosition
                      std::wstring(L"YES"));  // expectedIsNullable

  // Check 2nd Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),      // expectedCatalog
                      std::wstring(L"intTable"),  // expectedTable
                      std::wstring(L"id"),        // expectedColumn
                      SQL_BIGINT,                 // expectedDataType
                      std::wstring(L"BIGINT"),    // expectedTypeName
                      10,  // expectedColumnSize (mock returns 10 instead of 19)
                      8,   // expectedBufferLength
                      15,  // expectedDecimalDigits (mock returns 15 instead of 0)
                      10,  // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_BIGINT,             // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      8,                      // expectedOctetCharLength
                      1,                      // expectedOrdinalPosition
                      std::wstring(L"YES"));  // expectedIsNullable

  // There is no more column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

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

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),          // expectedCatalog
                      std::wstring(L"foreignTable"),  // expectedTable
                      std::wstring(L"id"),            // expectedColumn
                      SQL_BIGINT,                     // expectedDataType
                      std::wstring(L"BIGINT"),        // expectedTypeName
                      10,  // expectedColumnSize (mock returns 10 instead of 19)
                      8,   // expectedBufferLength
                      15,  // expectedDecimalDigits (mock returns 15 instead of 0)
                      10,  // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_BIGINT,             // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      8,                      // expectedOctetCharLength
                      1,                      // expectedOrdinalPosition
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

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributeAllTypes) {
  this->connect();
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColAttribute(this->stmt, 1,
                       std::wstring(L"bigint_col"),  // expectedColmnName
                       SQL_BIGINT,                   // expectedDataType
                       SQL_BIGINT,                   // expectedConciseType
                       20,                           // expectedDisplaySize
                       SQL_FALSE,                    // expectedPrecScale
                       8,                            // expectedLength
                       std::wstring(L""),            // expectedLiteralPrefix
                       std::wstring(L""),            // expectedLiteralSuffix
                       8,                            // expectedColumnSize
                       0,                            // expectedColumnScale
                       SQL_NULLABLE,                 // expectedColumnNullability
                       10,                           // expectedNumPrecRadix
                       8,                            // expectedOctetLength
                       SQL_PRED_NONE,                // expectedSearchable
                       SQL_FALSE);                   // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 2,
                       std::wstring(L"char_col"),  // expectedColmnName
                       SQL_WVARCHAR,               // expectedDataType
                       SQL_WVARCHAR,               // expectedConciseType
                       0,                          // expectedDisplaySize
                       SQL_FALSE,                  // expectedPrecScale
                       0,                          // expectedLength
                       std::wstring(L""),          // expectedLiteralPrefix
                       std::wstring(L""),          // expectedLiteralSuffix
                       0,                          // expectedColumnSize
                       0,                          // expectedColumnScale
                       SQL_NULLABLE,               // expectedColumnNullability
                       0,                          // expectedNumPrecRadix
                       0,                          // expectedOctetLength
                       SQL_PRED_NONE,              // expectedSearchable
                       SQL_TRUE);                  // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 3,
                       std::wstring(L"varbinary_col"),  // expectedColmnName
                       SQL_BINARY,                      // expectedDataType
                       SQL_BINARY,                      // expectedConciseType
                       0,                               // expectedDisplaySize
                       SQL_FALSE,                       // expectedPrecScale
                       0,                               // expectedLength
                       std::wstring(L""),               // expectedLiteralPrefix
                       std::wstring(L""),               // expectedLiteralSuffix
                       0,                               // expectedColumnSize
                       0,                               // expectedColumnScale
                       SQL_NULLABLE,                    // expectedColumnNullability
                       0,                               // expectedNumPrecRadix
                       0,                               // expectedOctetLength
                       SQL_PRED_NONE,                   // expectedSearchable
                       SQL_TRUE);                       // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 4,
                       std::wstring(L"double_col"),  // expectedColmnName
                       SQL_DOUBLE,                   // expectedDataType
                       SQL_DOUBLE,                   // expectedConciseType
                       24,                           // expectedDisplaySize
                       SQL_FALSE,                    // expectedPrecScale
                       8,                            // expectedLength
                       std::wstring(L""),            // expectedLiteralPrefix
                       std::wstring(L""),            // expectedLiteralSuffix
                       8,                            // expectedColumnSize
                       0,                            // expectedColumnScale
                       SQL_NULLABLE,                 // expectedColumnNullability
                       2,                            // expectedNumPrecRadix
                       8,                            // expectedOctetLength
                       SQL_PRED_NONE,                // expectedSearchable
                       SQL_FALSE);                   // expectedUnsignedColumn

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLColAttributeAllTypes) {
  // Test assumes there is a table $scratch.ODBCTest in remote server
  this->connect();

  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColAttribute(this->stmt, 1,
                       std::wstring(L"sinteger_max"),  // expectedColmnName
                       SQL_INTEGER,                    // expectedDataType
                       SQL_INTEGER,                    // expectedConciseType
                       11,                             // expectedDisplaySize
                       SQL_FALSE,                      // expectedPrecScale
                       4,                              // expectedLength
                       std::wstring(L""),              // expectedLiteralPrefix
                       std::wstring(L""),              // expectedLiteralSuffix
                       4,                              // expectedColumnSize
                       0,                              // expectedColumnScale
                       SQL_NULLABLE,                   // expectedColumnNullability
                       10,                             // expectedNumPrecRadix
                       4,                              // expectedOctetLength
                       SQL_PRED_NONE,                  // expectedSearchable
                       SQL_FALSE);                     // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 2,
                       std::wstring(L"sbigint_max"),  // expectedColmnName
                       SQL_BIGINT,                    // expectedDataType
                       SQL_BIGINT,                    // expectedConciseType
                       20,                            // expectedDisplaySize
                       SQL_FALSE,                     // expectedPrecScale
                       8,                             // expectedLength
                       std::wstring(L""),             // expectedLiteralPrefix
                       std::wstring(L""),             // expectedLiteralSuffix
                       8,                             // expectedColumnSize
                       0,                             // expectedColumnScale
                       SQL_NULLABLE,                  // expectedColumnNullability
                       10,                            // expectedNumPrecRadix
                       8,                             // expectedOctetLength
                       SQL_PRED_NONE,                 // expectedSearchable
                       SQL_FALSE);                    // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 3,
                       std::wstring(L"decimal_positive"),  // expectedColmnName
                       SQL_DECIMAL,                        // expectedDataType
                       SQL_DECIMAL,                        // expectedConciseType
                       40,                                 // expectedDisplaySize
                       SQL_FALSE,                          // expectedPrecScale
                       19,                                 // expectedLength
                       std::wstring(L""),                  // expectedLiteralPrefix
                       std::wstring(L""),                  // expectedLiteralSuffix
                       19,                                 // expectedColumnSize
                       0,                                  // expectedColumnScale
                       SQL_NULLABLE,                       // expectedColumnNullability
                       10,                                 // expectedNumPrecRadix
                       40,                                 // expectedOctetLength
                       SQL_PRED_NONE,                      // expectedSearchable
                       SQL_FALSE);                         // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 4,
                       std::wstring(L"float_max"),  // expectedColmnName
                       SQL_FLOAT,                   // expectedDataType
                       SQL_FLOAT,                   // expectedConciseType
                       24,                          // expectedDisplaySize
                       SQL_FALSE,                   // expectedPrecScale
                       8,                           // expectedLength
                       std::wstring(L""),           // expectedLiteralPrefix
                       std::wstring(L""),           // expectedLiteralSuffix
                       8,                           // expectedColumnSize
                       0,                           // expectedColumnScale
                       SQL_NULLABLE,                // expectedColumnNullability
                       2,                           // expectedNumPrecRadix
                       8,                           // expectedOctetLength
                       SQL_PRED_NONE,               // expectedSearchable
                       SQL_FALSE);                  // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 5,
                       std::wstring(L"double_max"),  // expectedColmnName
                       SQL_DOUBLE,                   // expectedDataType
                       SQL_DOUBLE,                   // expectedConciseType
                       24,                           // expectedDisplaySize
                       SQL_FALSE,                    // expectedPrecScale
                       8,                            // expectedLength
                       std::wstring(L""),            // expectedLiteralPrefix
                       std::wstring(L""),            // expectedLiteralSuffix
                       8,                            // expectedColumnSize
                       0,                            // expectedColumnScale
                       SQL_NULLABLE,                 // expectedColumnNullability
                       2,                            // expectedNumPrecRadix
                       8,                            // expectedOctetLength
                       SQL_PRED_NONE,                // expectedSearchable
                       SQL_FALSE);                   // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 6,
                       std::wstring(L"bit_true"),  // expectedColmnName
                       SQL_BIT,                    // expectedDataType
                       SQL_BIT,                    // expectedConciseType
                       1,                          // expectedDisplaySize
                       SQL_FALSE,                  // expectedPrecScale
                       1,                          // expectedLength
                       std::wstring(L""),          // expectedLiteralPrefix
                       std::wstring(L""),          // expectedLiteralSuffix
                       1,                          // expectedColumnSize
                       0,                          // expectedColumnScale
                       SQL_NULLABLE,               // expectedColumnNullability
                       0,                          // expectedNumPrecRadix
                       1,                          // expectedOctetLength
                       SQL_PRED_NONE,              // expectedSearchable
                       SQL_TRUE);                  // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 7,
                       std::wstring(L"date_max"),  // expectedColmnName
                       SQL_DATETIME,               // expectedDataType
                       SQL_TYPE_DATE,              // expectedConciseType
                       10,                         // expectedDisplaySize
                       SQL_FALSE,                  // expectedPrecScale
                       10,                         // expectedLength
                       std::wstring(L""),          // expectedLiteralPrefix
                       std::wstring(L""),          // expectedLiteralSuffix
                       10,                         // expectedColumnSize
                       0,                          // expectedColumnScale
                       SQL_NULLABLE,               // expectedColumnNullability
                       0,                          // expectedNumPrecRadix
                       6,                          // expectedOctetLength
                       SQL_PRED_NONE,              // expectedSearchable
                       SQL_TRUE);                  // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 8,
                       std::wstring(L"time_max"),  // expectedColmnName
                       SQL_DATETIME,               // expectedDataType
                       SQL_TYPE_TIME,              // expectedConciseType
                       12,                         // expectedDisplaySize
                       SQL_FALSE,                  // expectedPrecScale
                       12,                         // expectedLength
                       std::wstring(L""),          // expectedLiteralPrefix
                       std::wstring(L""),          // expectedLiteralSuffix
                       12,                         // expectedColumnSize
                       3,                          // expectedColumnScale
                       SQL_NULLABLE,               // expectedColumnNullability
                       0,                          // expectedNumPrecRadix
                       6,                          // expectedOctetLength
                       SQL_PRED_NONE,              // expectedSearchable
                       SQL_TRUE);                  // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 9,
                       std::wstring(L"timestamp_max"),  // expectedColmnName
                       SQL_DATETIME,                    // expectedDataType
                       SQL_TYPE_TIMESTAMP,              // expectedConciseType
                       23,                              // expectedDisplaySize
                       SQL_FALSE,                       // expectedPrecScale
                       23,                              // expectedLength
                       std::wstring(L""),               // expectedLiteralPrefix
                       std::wstring(L""),               // expectedLiteralSuffix
                       23,                              // expectedColumnSize
                       3,                               // expectedColumnScale
                       SQL_NULLABLE,                    // expectedColumnNullability
                       0,                               // expectedNumPrecRadix
                       16,                              // expectedOctetLength
                       SQL_PRED_NONE,                   // expectedSearchable
                       SQL_TRUE);                       // expectedUnsignedColumn

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLColAttributeAllTypesODBCVer2) {
  // Test assumes there is a table $scratch.ODBCTest in remote server
  this->connect(SQL_OV_ODBC2);

  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColAttribute(this->stmt, 1,
                       std::wstring(L"sinteger_max"),  // expectedColmnName
                       SQL_INTEGER,                    // expectedDataType
                       SQL_INTEGER,                    // expectedConciseType
                       11,                             // expectedDisplaySize
                       SQL_FALSE,                      // expectedPrecScale
                       4,                              // expectedLength
                       std::wstring(L""),              // expectedLiteralPrefix
                       std::wstring(L""),              // expectedLiteralSuffix
                       4,                              // expectedColumnSize
                       0,                              // expectedColumnScale
                       SQL_NULLABLE,                   // expectedColumnNullability
                       10,                             // expectedNumPrecRadix
                       4,                              // expectedOctetLength
                       SQL_PRED_NONE,                  // expectedSearchable
                       SQL_FALSE);                     // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 2,
                       std::wstring(L"sbigint_max"),  // expectedColmnName
                       SQL_BIGINT,                    // expectedDataType
                       SQL_BIGINT,                    // expectedConciseType
                       20,                            // expectedDisplaySize
                       SQL_FALSE,                     // expectedPrecScale
                       8,                             // expectedLength
                       std::wstring(L""),             // expectedLiteralPrefix
                       std::wstring(L""),             // expectedLiteralSuffix
                       8,                             // expectedColumnSize
                       0,                             // expectedColumnScale
                       SQL_NULLABLE,                  // expectedColumnNullability
                       10,                            // expectedNumPrecRadix
                       8,                             // expectedOctetLength
                       SQL_PRED_NONE,                 // expectedSearchable
                       SQL_FALSE);                    // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 3,
                       std::wstring(L"decimal_positive"),  // expectedColmnName
                       SQL_DECIMAL,                        // expectedDataType
                       SQL_DECIMAL,                        // expectedConciseType
                       40,                                 // expectedDisplaySize
                       SQL_FALSE,                          // expectedPrecScale
                       19,                                 // expectedLength
                       std::wstring(L""),                  // expectedLiteralPrefix
                       std::wstring(L""),                  // expectedLiteralSuffix
                       19,                                 // expectedColumnSize
                       0,                                  // expectedColumnScale
                       SQL_NULLABLE,                       // expectedColumnNullability
                       10,                                 // expectedNumPrecRadix
                       40,                                 // expectedOctetLength
                       SQL_PRED_NONE,                      // expectedSearchable
                       SQL_FALSE);                         // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 4,
                       std::wstring(L"float_max"),  // expectedColmnName
                       SQL_FLOAT,                   // expectedDataType
                       SQL_FLOAT,                   // expectedConciseType
                       24,                          // expectedDisplaySize
                       SQL_FALSE,                   // expectedPrecScale
                       8,                           // expectedLength
                       std::wstring(L""),           // expectedLiteralPrefix
                       std::wstring(L""),           // expectedLiteralSuffix
                       8,                           // expectedColumnSize
                       0,                           // expectedColumnScale
                       SQL_NULLABLE,                // expectedColumnNullability
                       2,                           // expectedNumPrecRadix
                       8,                           // expectedOctetLength
                       SQL_PRED_NONE,               // expectedSearchable
                       SQL_FALSE);                  // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 5,
                       std::wstring(L"double_max"),  // expectedColmnName
                       SQL_DOUBLE,                   // expectedDataType
                       SQL_DOUBLE,                   // expectedConciseType
                       24,                           // expectedDisplaySize
                       SQL_FALSE,                    // expectedPrecScale
                       8,                            // expectedLength
                       std::wstring(L""),            // expectedLiteralPrefix
                       std::wstring(L""),            // expectedLiteralSuffix
                       8,                            // expectedColumnSize
                       0,                            // expectedColumnScale
                       SQL_NULLABLE,                 // expectedColumnNullability
                       2,                            // expectedNumPrecRadix
                       8,                            // expectedOctetLength
                       SQL_PRED_NONE,                // expectedSearchable
                       SQL_FALSE);                   // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 6,
                       std::wstring(L"bit_true"),  // expectedColmnName
                       SQL_BIT,                    // expectedDataType
                       SQL_BIT,                    // expectedConciseType
                       1,                          // expectedDisplaySize
                       SQL_FALSE,                  // expectedPrecScale
                       1,                          // expectedLength
                       std::wstring(L""),          // expectedLiteralPrefix
                       std::wstring(L""),          // expectedLiteralSuffix
                       1,                          // expectedColumnSize
                       0,                          // expectedColumnScale
                       SQL_NULLABLE,               // expectedColumnNullability
                       0,                          // expectedNumPrecRadix
                       1,                          // expectedOctetLength
                       SQL_PRED_NONE,              // expectedSearchable
                       SQL_TRUE);                  // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 7,
                       std::wstring(L"date_max"),  // expectedColmnName
                       SQL_DATETIME,               // expectedDataType
                       SQL_DATE,                   // expectedConciseType
                       10,                         // expectedDisplaySize
                       SQL_FALSE,                  // expectedPrecScale
                       10,                         // expectedLength
                       std::wstring(L""),          // expectedLiteralPrefix
                       std::wstring(L""),          // expectedLiteralSuffix
                       10,                         // expectedColumnSize
                       0,                          // expectedColumnScale
                       SQL_NULLABLE,               // expectedColumnNullability
                       0,                          // expectedNumPrecRadix
                       6,                          // expectedOctetLength
                       SQL_PRED_NONE,              // expectedSearchable
                       SQL_TRUE);                  // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 8,
                       std::wstring(L"time_max"),  // expectedColmnName
                       SQL_DATETIME,               // expectedDataType
                       SQL_TIME,                   // expectedConciseType
                       12,                         // expectedDisplaySize
                       SQL_FALSE,                  // expectedPrecScale
                       12,                         // expectedLength
                       std::wstring(L""),          // expectedLiteralPrefix
                       std::wstring(L""),          // expectedLiteralSuffix
                       12,                         // expectedColumnSize
                       3,                          // expectedColumnScale
                       SQL_NULLABLE,               // expectedColumnNullability
                       0,                          // expectedNumPrecRadix
                       6,                          // expectedOctetLength
                       SQL_PRED_NONE,              // expectedSearchable
                       SQL_TRUE);                  // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 9,
                       std::wstring(L"timestamp_max"),  // expectedColmnName
                       SQL_DATETIME,                    // expectedDataType
                       SQL_TIMESTAMP,                   // expectedConciseType
                       23,                              // expectedDisplaySize
                       SQL_FALSE,                       // expectedPrecScale
                       23,                              // expectedLength
                       std::wstring(L""),               // expectedLiteralPrefix
                       std::wstring(L""),               // expectedLiteralSuffix
                       23,                              // expectedColumnSize
                       3,                               // expectedColumnScale
                       SQL_NULLABLE,                    // expectedColumnNullability
                       0,                               // expectedNumPrecRadix
                       16,                              // expectedOctetLength
                       SQL_PRED_NONE,                   // expectedSearchable
                       SQL_TRUE);                       // expectedUnsignedColumn

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributeUniqueValue) {
  // Mock server limitation: returns false for auto-increment column
  this->connect();
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  checkSQLColAttributeNumeric(this->stmt, wsql, 1, SQL_DESC_AUTO_UNIQUE_VALUE, SQL_FALSE);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributeBaseTableName) {
  this->connect();
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  checkSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_BASE_TABLE_NAME,
                             std::wstring(L"AllTypesTable"));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributeCatalogName) {
  // Mock server limitattion: mock doesn't return catalog for result metadata,
  // and the defautl catalog should be 'main'
  this->connect();
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  checkSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_CATALOG_NAME,
                             std::wstring(L""));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributeCount) {
  this->connect();
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  // Pass 0 as column number, driver should ignore it
  checkSQLColAttributeNumeric(this->stmt, wsql, 0, SQL_DESC_COUNT, 4);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributeLocalTypeName) {
  this->connect();
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  // Mock server doesn't have local type name
  checkSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_LOCAL_TYPE_NAME,
                             std::wstring(L""));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributeSchemaName) {
  this->connect();
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  // Mock server doesn't have schemas
  checkSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_SCHEMA_NAME,
                             std::wstring(L""));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributeTableName) {
  this->connect();
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  checkSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_TABLE_NAME,
                             std::wstring(L"AllTypesTable"));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributeTypeName) {
  this->connect();
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  // Mock server doesn't return data source-dependent data type name
  checkSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_TYPE_NAME, std::wstring(L""));

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLColAttributeTypeName) {
  this->connect();

  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  checkSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_TYPE_NAME,
                             std::wstring(L"INTEGER"));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributeUnnamed) {
  this->connect();
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  checkSQLColAttributeNumeric(this->stmt, wsql, 1, SQL_DESC_UNNAMED, SQL_NAMED);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributeUpdatable) {
  this->connect();
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  // Mock server does not return updatable information
  checkSQLColAttributeNumeric(this->stmt, wsql, 1, SQL_DESC_UPDATABLE,
                              SQL_ATTR_READWRITE_UNKNOWN);

  this->disconnect();
}
}  // namespace arrow::flight::sql::odbc
