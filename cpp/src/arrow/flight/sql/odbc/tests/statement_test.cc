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

#include <limits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace arrow::flight::sql::odbc {

TYPED_TEST(FlightSQLODBCTestBase, TestSQLExecDirectSimpleQuery) {
  this->connect();

  std::wstring wsql = L"SELECT 1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLINTEGER val;
  SQLLEN bufLen = sizeof(val);

  ret = SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Verify 1 is returned
  EXPECT_EQ(val, 1);

  ret = SQLFetch(stmt);

  EXPECT_EQ(ret, SQL_NO_DATA);

  ret = SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_ERROR);
  // Invalid cursor state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_24000);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLExecDirectInvalidQuery) {
  this->connect();

  std::wstring wsql = L"SELECT;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));

  EXPECT_EQ(ret, SQL_ERROR);
  // ODBC provides generic error code HY000 to all statement errors
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_HY000);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLExecDirectDataQuery) {
  this->connect();

  std::wstring wsql = this->getQueryAllDataTypes();
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  if (ret != SQL_SUCCESS) {
    // -AL- remove later
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_STMT, this->stmt) << std::endl;
  }

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Numeric Types

  // Signed Tiny Int
  int8_t stiny_int_val;
  SQLLEN bufLen = sizeof(stiny_int_val);

  ret = SQLGetData(this->stmt, 1, SQL_C_STINYINT, &stiny_int_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(stiny_int_val, std::numeric_limits<int8_t>::min());

  ret = SQLGetData(this->stmt, 2, SQL_C_STINYINT, &stiny_int_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(stiny_int_val, std::numeric_limits<int8_t>::max());

  // Unsigned Tiny Int
  uint8_t utiny_int_val;
  bufLen = sizeof(utiny_int_val);

  ret = SQLGetData(this->stmt, 3, SQL_C_UTINYINT, &utiny_int_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(utiny_int_val, std::numeric_limits<uint8_t>::min());

  ret = SQLGetData(this->stmt, 4, SQL_C_UTINYINT, &utiny_int_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(utiny_int_val, std::numeric_limits<uint8_t>::max());

  // Signed Small Int
  int16_t ssmall_int_val;
  bufLen = sizeof(ssmall_int_val);

  ret = SQLGetData(this->stmt, 5, SQL_C_SSHORT, &ssmall_int_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ssmall_int_val, std::numeric_limits<int16_t>::min());

  ret = SQLGetData(this->stmt, 6, SQL_C_SSHORT, &ssmall_int_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ssmall_int_val, std::numeric_limits<int16_t>::max());

  // Unsigned Small Int
  uint16_t usmall_int_val;
  bufLen = sizeof(usmall_int_val);

  ret = SQLGetData(this->stmt, 7, SQL_C_USHORT, &usmall_int_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(usmall_int_val, std::numeric_limits<uint16_t>::min());

  ret = SQLGetData(this->stmt, 8, SQL_C_USHORT, &usmall_int_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(usmall_int_val, std::numeric_limits<uint16_t>::max());

  // Signed Integer
  SQLINTEGER slong_val;
  bufLen = sizeof(slong_val);

  ret = SQLGetData(this->stmt, 9, SQL_C_SLONG, &slong_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(slong_val, std::numeric_limits<SQLINTEGER>::min());

  ret = SQLGetData(this->stmt, 10, SQL_C_SLONG, &slong_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(slong_val, std::numeric_limits<SQLINTEGER>::max());

  // Unsigned Integer
  SQLUINTEGER ulong_val;
  bufLen = sizeof(ulong_val);

  ret = SQLGetData(this->stmt, 11, SQL_C_ULONG, &ulong_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ulong_val, std::numeric_limits<SQLUINTEGER>::min());

  ret = SQLGetData(this->stmt, 12, SQL_C_ULONG, &ulong_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ulong_val, std::numeric_limits<SQLUINTEGER>::max());

  // Signed Big Int
  SQLBIGINT sbig_int_val;
  bufLen = sizeof(sbig_int_val);

  ret = SQLGetData(this->stmt, 13, SQL_C_SBIGINT, &sbig_int_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(sbig_int_val, std::numeric_limits<SQLBIGINT>::min());

  ret = SQLGetData(this->stmt, 14, SQL_C_SBIGINT, &sbig_int_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(sbig_int_val, std::numeric_limits<SQLBIGINT>::max());

  // Unsigned Big Int
  SQLUBIGINT ubig_int_val;
  bufLen = sizeof(ubig_int_val);

  ret = SQLGetData(this->stmt, 15, SQL_C_UBIGINT, &ubig_int_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ubig_int_val, std::numeric_limits<SQLUBIGINT>::min());

  ret = SQLGetData(this->stmt, 16, SQL_C_UBIGINT, &ubig_int_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ubig_int_val, std::numeric_limits<SQLUBIGINT>::max());

  // Decimal Types
  /*
  * //-AL- todo circle back to this later.
  * //-AL- need for both
  //  CAST(-999999999.99 AS DECIMAL(18, 2)) AS decimal_min,
  //        CAST(999999999.99 AS DECIMAL(18, 2)) AS decimal_max,

  SQL_NUMERIC_STRUCT decimal;
  memset(&decimal, 0, sizeof(decimal));

  //num.sign = 1;
  //num.precision = 1;

    bufLen = sizeof(SQL_NUMERIC_STRUCT);

  ret = SQLGetData(this->stmt, 17, SQL_C_NUMERIC, &decimal, bufLen, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  if (ret != SQL_SUCCESS) {
    // -AL- remove later
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_STMT, this->stmt) << std::endl;
  }
  EXPECT_EQ(decimal.sign, 1);
  EXPECT_EQ(decimal.scale, 1);
  EXPECT_EQ(decimal.precision, 38);
  EXPECT_THAT(decimal.val, ::testing::ElementsAre(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                                  0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0));

 // -AL- TODO and 1 more for row 18
*/

  // Float
  float float_val;
  bufLen = sizeof(float_val);

  ret = SQLGetData(this->stmt, 19, SQL_C_FLOAT, &float_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Get minimum negative float value
  EXPECT_EQ(float_val, -std::numeric_limits<float>::max());

  ret = SQLGetData(this->stmt, 20, SQL_C_FLOAT, &float_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(float_val, std::numeric_limits<float>::max());

  // Double
  SQLDOUBLE double_val;
  bufLen = sizeof(double_val);

  ret = SQLGetData(this->stmt, 21, SQL_C_DOUBLE, &double_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Get minimum negative double value
  EXPECT_EQ(double_val, -std::numeric_limits<SQLDOUBLE>::max());

  ret = SQLGetData(this->stmt, 22, SQL_C_DOUBLE, &double_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(double_val, std::numeric_limits<SQLDOUBLE>::max());

  // Bit
  bool bit_val;
  bufLen = sizeof(bit_val);

  ret = SQLGetData(this->stmt, 23, SQL_C_BIT, &bit_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(bit_val, false);

  ret = SQLGetData(this->stmt, 24, SQL_C_BIT, &bit_val, 0, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(bit_val, true);

  // Characters

  // Char
  SQLCHAR char_val[2];
  bufLen = sizeof(char_val) * 2;

  ret = SQLGetData(this->stmt, 25, SQL_C_CHAR, &char_val, bufLen, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(char_val[0], 'Z');

  // WChar
  SQLWCHAR wchar_val[2];
  bufLen = sizeof(wchar_val) * 2;

  ret = SQLGetData(this->stmt, 26, SQL_C_WCHAR, &wchar_val, bufLen, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(wchar_val[0], L'你');

  // WVarchar
  SQLWCHAR wvarchar_val[3];
  bufLen = sizeof(wvarchar_val) * 3;

  ret = SQLGetData(this->stmt, 27, SQL_C_WCHAR, &wvarchar_val, bufLen, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(wvarchar_val[0], L'你');
  EXPECT_EQ(wvarchar_val[1], L'好');

  // varchar
  SQLCHAR varchar_val[4];
  bufLen = sizeof(varchar_val) * 4;

  ret = SQLGetData(this->stmt, 28, SQL_C_CHAR, &varchar_val, bufLen, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(varchar_val[0], 'X');
  EXPECT_EQ(varchar_val[1], 'Y');
  EXPECT_EQ(varchar_val[2], 'Z');

  // Date and Time, Timestamp

  SQL_DATE_STRUCT date_var{};
  bufLen = sizeof(date_var);

  ret = SQLGetData(this->stmt, 29, SQL_C_TYPE_DATE, &date_var, bufLen, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check min values for date. Min valid year is 1400.
  EXPECT_EQ(date_var.day, 1);
  EXPECT_EQ(date_var.month, 1);
  EXPECT_EQ(date_var.year, 1400);

  ret = SQLGetData(this->stmt, 30, SQL_C_TYPE_DATE, &date_var, bufLen, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check max values for date. Max valid year is 9999.
  EXPECT_EQ(date_var.day, 31);
  EXPECT_EQ(date_var.month, 12);
  EXPECT_EQ(date_var.year, 9999);

  // -AL- todo add more checks

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLExecDirectTEMPQuery) {
  // -AL- TEMP test for date,it works.
  this->connect();

  std::wstring wsql = L"SELECT DATE('2024-01-23') AS date, DATE('1920-03-07') AS date2;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // date
  SQL_DATE_STRUCT date_var{};
  SQLLEN bufLen = sizeof(date_var);

  ret = SQLGetData(this->stmt, 1, SQL_C_TYPE_DATE, &date_var, bufLen, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check min values for date
  EXPECT_EQ(date_var.day, 23);
  EXPECT_EQ(date_var.month, 1);
  EXPECT_EQ(date_var.year, 2024);

  // date 2
  ret = SQLGetData(this->stmt, 2, SQL_C_TYPE_DATE, &date_var, bufLen, &bufLen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check min values for date
  EXPECT_EQ(date_var.day, 7);
  EXPECT_EQ(date_var.month, 3);
  EXPECT_EQ(date_var.year, 1920);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLExecDirectVarbinaryQuery) {
  // Have binary test on mock test base as remote test servers tend to have different
  // formats for binary data
  this->connect();

  std::wstring wsql = L"SELECT X'ABCDEF' AS c_varbinary;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // varbinary
  std::vector<int8_t> varbinary_val(3);
  SQLLEN bufLen = varbinary_val.size();
  ret = SQLGetData(this->stmt, 1, SQL_C_BINARY, &varbinary_val[0], bufLen, &bufLen);
  EXPECT_EQ(varbinary_val[0], '\xAB');
  EXPECT_EQ(varbinary_val[1], '\xCD');
  EXPECT_EQ(varbinary_val[2], '\xEF');

  this->disconnect();
}

//-AL- todo add checks for fetching a table with many rows.

}  // namespace arrow::flight::sql::odbc
