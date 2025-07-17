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
/**
 * Check string column.
 *
 * @param stmt Statement.
 * @param colId Column ID to check.
 * @param value Expected value.
 */
void CheckStringColumn(SQLHSTMT stmt, int colId, const std::string& value) {
  char buf[1024];
  SQLLEN bufLen = sizeof(buf);

  SQLRETURN ret = SQLGetData(stmt, colId, SQL_C_CHAR, buf, sizeof(buf), &bufLen);
  EXPECT_EQ(ret, SQL_SUCCESS);
  if (ret != SQL_SUCCESS) {
    // -AL- temp
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt) << std::endl;
  }

  if (bufLen <= 0)
    EXPECT_TRUE(value.empty());
  else
    EXPECT_EQ(std::string(buf, static_cast<size_t>(bufLen)), value);
}

/**
 * Check int column.
 *
 * @param stmt Statement.
 * @param colId Column ID to check.
 * @param value Expected value.
 */
void CheckIntColumn(SQLHSTMT stmt, int colId, const SQLINTEGER& value) {
  SQLINTEGER buf;
  SQLLEN bufLen = sizeof(buf);

  SQLRETURN ret = SQLGetData(stmt, colId, SQL_C_LONG, &buf, sizeof(buf), &bufLen);
  EXPECT_EQ(ret, SQL_SUCCESS);
  if (ret != SQL_SUCCESS) {
    // -AL- temp
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt) << std::endl;
  }

  EXPECT_EQ(buf, value);
}

/**
 * Check smallint column.
 *
 * @param stmt Statement.
 * @param colId Column ID to check.
 * @param value Expected value.
 */
void CheckSmallIntColumn(SQLHSTMT stmt, int colId, const SQLSMALLINT& value) {
  SQLSMALLINT buf;
  SQLLEN bufLen = sizeof(buf);

  SQLRETURN ret = SQLGetData(stmt, colId, SQL_C_SSHORT, &buf, sizeof(buf), &bufLen);
  EXPECT_EQ(ret, SQL_SUCCESS);
  if (ret != SQL_SUCCESS) {
    // -AL- temp
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt) << std::endl;
  }

  EXPECT_EQ(buf, value);
}

// -AL- TODO: work on fetching all columns. The plan is to use this test as example,
// -AL- next TODO: create table that supports all SQLite data types,
// and change table pattern to match that one table. Use a new test to verify all types.

//  if (boost::iequals(sqlite_type, "int") || boost::iequals(sqlite_type, "integer")) {
//  return int64();
//} else if (boost::iequals(sqlite_type, "REAL")) {
//  return float64();
//} else if (boost::iequals(sqlite_type, "BLOB")) {
//  return binary();
//} else if (boost::iequals(sqlite_type, "TEXT") || boost::iequals(sqlite_type, "DATE") ||
//           boost::istarts_with(sqlite_type, "char") ||
//           boost::istarts_with(sqlite_type, "varchar")) {
//  return utf8();
//}
TEST_F(FlightSQLODBCMockTestBase, TestSQLColumns) {
  this->connect();

  // Attempt to get all columns
  SQLWCHAR tablePattern[] = L"%";
  SQLWCHAR columnPattern[] = L"%";

  SQLRETURN ret = SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, tablePattern,
                             SQL_NTS, columnPattern, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);
  if (ret != SQL_SUCCESS) {
    // -AL- temp
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_STMT, this->stmt) << std::endl;
  }

  ret = SQLFetch(stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);
  if (ret != SQL_SUCCESS) {
    // -AL- temp
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_STMT, this->stmt) << std::endl;
  }

  std::string empty = std::string("");

  // 18 columns are returned by SQLColumns
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
  this->disconnect();
}

}  // namespace arrow::flight::sql::odbc
