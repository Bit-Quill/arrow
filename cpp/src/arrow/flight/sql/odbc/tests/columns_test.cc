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

  std::string empty = std::string("");

  // 18 columns are returned by SQLColumns, check first column thoroughly
  CheckStringColumn(this->stmt, 1, std::string("main"));          // catalog
  CheckStringColumn(this->stmt, 2, empty);                        // schema
  CheckStringColumn(this->stmt, 3, std::string("foreignTable"));  // table name
  CheckStringColumn(this->stmt, 4, std::string("id"));            // column name

  CheckIntColumn(this->stmt, 5, SQL_BIGINT);  // data type

  CheckStringColumn(this->stmt, 6, std::string("BIGINT"));  // type name

  // mock limitation: SQLite mock server returns 10 for bigint size when spec indicates
  // should be 19
  CheckIntColumn(this->stmt, 7, 10);  // column size
  CheckIntColumn(this->stmt, 8, 8);   // buffer length

  // DECIMAL_DIGITS should be 0 for bigint type since it is exact
  // mock limitation: SQLite mock server returns 10 for bigint decimal digits when spec
  // indicates should be 0
  CheckSmallIntColumn(this->stmt, 9, 15);   // decimal digits
  CheckSmallIntColumn(this->stmt, 10, 10);  // num prec radix
  CheckSmallIntColumn(this->stmt, 11,
                      SQL_NULLABLE);  // nullable

  CheckStringColumn(this->stmt, 12, empty);  // remarks
  CheckStringColumn(this->stmt, 13, empty);  // column def

  CheckSmallIntColumn(this->stmt, 14, SQL_BIGINT);  // sql data type  not NULL
  CheckSmallIntColumn(this->stmt, 15, NULL);        // sql date type sub
  CheckIntColumn(this->stmt, 16, 8);                // char octet length
  CheckIntColumn(this->stmt, 17,
                 1);  // oridinal position

  CheckStringColumn(this->stmt, 18, std::string("YES"));  // is nullable

  // Check 2nd Column, only check table and column name
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckStringColumn(this->stmt, 3, std::string("foreignTable"));  // table name
  CheckStringColumn(this->stmt, 4, std::string("foreignName"));   // column name

  // Check 3rd Column, only check table and column name
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckStringColumn(this->stmt, 3, std::string("foreignTable"));  // table name
  CheckStringColumn(this->stmt, 4, std::string("value"));         // column name

  // Check 4th Column, only check table and column name
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckStringColumn(this->stmt, 3, std::string("intTable"));  // table name
  CheckStringColumn(this->stmt, 4, std::string("id"));        // column name

  // Check 5th Column, only check table and column name
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckStringColumn(this->stmt, 3, std::string("intTable"));  // table name
  CheckStringColumn(this->stmt, 4, std::string("keyName"));   // column name

  // Check 6th Column, only check table and column name
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckStringColumn(this->stmt, 3, std::string("intTable"));  // table name
  CheckStringColumn(this->stmt, 4, std::string("value"));     // column name

  // Check 7th Column, only check table and column name
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckStringColumn(this->stmt, 3, std::string("intTable"));   // table name
  CheckStringColumn(this->stmt, 4, std::string("foreignId"));  // column name

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColumnsAllTypes) {
  // Limitation: Mock server returns incorrect values for column size for some columns.
  // For character and binary type columns, the driver calculates buffer length and char
  // octet length from column size.

  // Checks filtering table with table name pattern
  this->connect();
  this->CreateTableAllDataType();

  // Attempt to get all columns
  SQLWCHAR tablePattern[] = L"AllTypesTable";
  SQLWCHAR columnPattern[] = L"%";

  SQLRETURN ret = SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, tablePattern,
                             SQL_NTS, columnPattern, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Fetch SQLColumn data for 1st column in AllTypesTable
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  std::string empty = std::string("");

  CheckStringColumn(this->stmt, 1, std::string("main"));           // catalog
  CheckStringColumn(this->stmt, 2, empty);                         // schema
  CheckStringColumn(this->stmt, 3, std::string("AllTypesTable"));  // table name
  CheckStringColumn(this->stmt, 4, std::string("bigint_col"));     // column name

  CheckIntColumn(this->stmt, 5, SQL_BIGINT);  // data type

  CheckStringColumn(this->stmt, 6, std::string("BIGINT"));  // type name

  // mock limitation: SQLite mock server returns 10 for bigint column size when spec
  // indicates should be 19
  CheckIntColumn(this->stmt, 7, 10);  // column size
  CheckIntColumn(this->stmt, 8, 8);   // buffer length

  // DECIMAL_DIGITS should be 0 for bigint type since it is exact
  // mock limitation: SQLite mock server returns 10 for bigint decimal digits when spec
  // indicates should be 0
  CheckSmallIntColumn(this->stmt, 9, 15);   // decimal digits
  CheckSmallIntColumn(this->stmt, 10, 10);  // num prec radix
  CheckSmallIntColumn(this->stmt, 11,
                      SQL_NULLABLE);  // nullable

  CheckStringColumn(this->stmt, 12, empty);  // remarks
  CheckStringColumn(this->stmt, 13, empty);  // column def

  CheckSmallIntColumn(this->stmt, 14, SQL_BIGINT);  // sql data type  not NULL
  CheckSmallIntColumn(this->stmt, 15, NULL);        // sql date type sub
  CheckIntColumn(this->stmt, 16, 8);                // char octet length
  CheckIntColumn(this->stmt, 17, 1);                // oridinal position

  CheckStringColumn(this->stmt, 18, std::string("YES"));  // is nullable

  // Check SQLColumn data for 2nd column in AllTypesTable
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckStringColumn(this->stmt, 1, std::string("main"));           // catalog
  CheckStringColumn(this->stmt, 2, empty);                         // schema
  CheckStringColumn(this->stmt, 3, std::string("AllTypesTable"));  // table name
  CheckStringColumn(this->stmt, 4, std::string("char_col"));       // column name

  CheckIntColumn(this->stmt, 5, SQL_WVARCHAR);  // data type

  CheckStringColumn(this->stmt, 6, std::string("WVARCHAR"));  // type name

  // mock limitation: SQLite mock server returns 0 for varchar(100) column size when spec
  // indicates should be 100.
  CheckIntColumn(this->stmt, 7, 0);  // column size
  CheckIntColumn(this->stmt, 8, 0);  // buffer length

  CheckSmallIntColumn(this->stmt, 9, 15);  // decimal digits
  CheckSmallIntColumn(this->stmt, 10, 0);  // num prec radix
  CheckSmallIntColumn(this->stmt, 11,
                      SQL_NULLABLE);  // nullable

  CheckStringColumn(this->stmt, 12, empty);  // remarks
  CheckStringColumn(this->stmt, 13, empty);  // column def

  CheckSmallIntColumn(this->stmt, 14, SQL_WVARCHAR);  // sql data type  not NULL
  CheckSmallIntColumn(this->stmt, 15, NULL);          // sql date type sub
  CheckIntColumn(this->stmt, 16, 0);                  // char octet length
  CheckIntColumn(this->stmt, 17, 2);                  // oridinal position

  CheckStringColumn(this->stmt, 18, std::string("YES"));  // is nullable

  // Check SQLColumn data for 3rd column in AllTypesTable
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckStringColumn(this->stmt, 1, std::string("main"));           // catalog
  CheckStringColumn(this->stmt, 2, empty);                         // schema
  CheckStringColumn(this->stmt, 3, std::string("AllTypesTable"));  // table name
  CheckStringColumn(this->stmt, 4, std::string("varbinary_col"));  // column name

  CheckIntColumn(this->stmt, 5, SQL_BINARY);  // data type

  CheckStringColumn(this->stmt, 6, std::string("BINARY"));  // type name

  // mock limitation: SQLite mock server returns 0 for BLOB column size when spec
  // indicates should be binary data limit.
  CheckIntColumn(this->stmt, 7, 0);  // column size
  CheckIntColumn(this->stmt, 8, 0);  // buffer length

  CheckSmallIntColumn(this->stmt, 9, 15);  // decimal digits
  CheckSmallIntColumn(this->stmt, 10, 0);  // num prec radix
  CheckSmallIntColumn(this->stmt, 11,
                      SQL_NULLABLE);  // nullable

  CheckStringColumn(this->stmt, 12, empty);  // remarks
  CheckStringColumn(this->stmt, 13, empty);  // column def

  CheckSmallIntColumn(this->stmt, 14, SQL_BINARY);  // sql data type  not NULL
  CheckSmallIntColumn(this->stmt, 15, NULL);        // sql date type sub
  CheckIntColumn(this->stmt, 16, 0);                // char octet length
  CheckIntColumn(this->stmt, 17, 3);                // oridinal position

  CheckStringColumn(this->stmt, 18, std::string("YES"));  // is nullable

  // Check SQLColumn data for 4th column in AllTypesTable
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckStringColumn(this->stmt, 1, std::string("main"));           // catalog
  CheckStringColumn(this->stmt, 2, empty);                         // schema
  CheckStringColumn(this->stmt, 3, std::string("AllTypesTable"));  // table name
  CheckStringColumn(this->stmt, 4, std::string("double_col"));     // column name

  CheckIntColumn(this->stmt, 5, SQL_DOUBLE);  // data type

  CheckStringColumn(this->stmt, 6, std::string("DOUBLE"));  // type name

  CheckIntColumn(this->stmt, 7, 15);  // column size
  CheckIntColumn(this->stmt, 8, 8);   // buffer length

  CheckSmallIntColumn(this->stmt, 9, 15);  // decimal digits
  CheckSmallIntColumn(this->stmt, 10, 2);  // num prec radix
  CheckSmallIntColumn(this->stmt, 11,
                      SQL_NULLABLE);  // nullable

  CheckStringColumn(this->stmt, 12, empty);  // remarks
  CheckStringColumn(this->stmt, 13, empty);  // column def

  CheckSmallIntColumn(this->stmt, 14, SQL_DOUBLE);  // sql data type  not NULL
  CheckSmallIntColumn(this->stmt, 15, NULL);        // sql date type sub
  CheckIntColumn(this->stmt, 16, 8);                // char octet length
  CheckIntColumn(this->stmt, 17, 4);                // oridinal position

  CheckStringColumn(this->stmt, 18, std::string("YES"));  // is nullable

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

  // Skip the check for 1st column in unicode table 数据
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  std::string empty = std::string("");

  // Check SQLColumn data for 2nd column in AllTypesTable
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckStringColumn(this->stmt, 1, std::string("main"));     // catalog
  CheckStringColumn(this->stmt, 2, empty);                   // schema
  CheckStringColumnW(this->stmt, 3, std::wstring(L"数据"));  // table name
  CheckStringColumnW(this->stmt, 4, std::wstring(L"资料"));  // column name

  CheckIntColumn(this->stmt, 5, SQL_WVARCHAR);  // data type

  CheckStringColumn(this->stmt, 6, std::string("WVARCHAR"));  // type name

  // mock limitation: SQLite mock server returns 0 for varchar(100) column size when spec
  // indicates should be 100.
  CheckIntColumn(this->stmt, 7, 0);  // column size
  CheckIntColumn(this->stmt, 8, 0);  // buffer length

  CheckSmallIntColumn(this->stmt, 9, 15);  // decimal digits
  CheckSmallIntColumn(this->stmt, 10, 0);  // num prec radix
  CheckSmallIntColumn(this->stmt, 11,
                      SQL_NULLABLE);  // nullable

  CheckStringColumn(this->stmt, 12, empty);  // remarks
  CheckStringColumn(this->stmt, 13, empty);  // column def

  CheckSmallIntColumn(this->stmt, 14, SQL_WVARCHAR);  // sql data type  not NULL
  CheckSmallIntColumn(this->stmt, 15, NULL);          // sql date type sub
  CheckIntColumn(this->stmt, 16, 0);                  // char octet length
  CheckIntColumn(this->stmt, 17, 2);                  // oridinal position

  CheckStringColumn(this->stmt, 18, std::string("YES"));  // is nullable

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

  std::string empty = std::string("");

  CheckStringColumn(this->stmt, 3, std::string("foreignTable"));  // table name
  CheckStringColumn(this->stmt, 4, std::string("id"));            // column name

  // Check 2nd Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckStringColumn(this->stmt, 3, std::string("intTable"));  // table name
  CheckStringColumn(this->stmt, 4, std::string("id"));        // column name

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

  std::string empty = std::string("");

  CheckStringColumn(this->stmt, 3, std::string("foreignTable"));  // table name
  CheckStringColumn(this->stmt, 4, std::string("id"));            // column name

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
