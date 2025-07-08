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

// Helper Functions

// Validate unsigned length SQLULEN return value
void validateGetStmtAttr(SQLHSTMT statement, SQLUSMALLINT attribute,
                         SQLULEN expected_value) {
  SQLULEN value = 0;
  SQLINTEGER stringLengthPtr;

  SQLRETURN ret = SQLGetStmtAttr(statement, attribute, &value, 0, &stringLengthPtr);

  if (ret != SQL_SUCCESS) {
    // TODO remove later
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_STMT, statement) << std::endl;
  }

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(value, expected_value);
}

// Validate unsigned length SQLULEN return value is greater than
void validateGetStmtAttrGreaterThan(SQLHSTMT statement, SQLUSMALLINT attribute,
                                    SQLULEN compared_value) {
  SQLULEN value = 0;
  SQLINTEGER stringLengthPtr;

  SQLRETURN ret = SQLGetStmtAttr(statement, attribute, &value, 0, &stringLengthPtr);

  if (ret != SQL_SUCCESS) {
    // TODO remove later
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_STMT, statement) << std::endl;
  }

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(value, compared_value);
}

// Validate error return value and code
void validateGetStmtAttrErrorCode(SQLHSTMT statement, SQLUSMALLINT attribute,
                                  std::string_view error_code) {
  SQLULEN value = 0;
  SQLINTEGER stringLengthPtr;

  SQLRETURN ret = SQLGetStmtAttr(statement, attribute, &value, 0, &stringLengthPtr);

  EXPECT_EQ(ret, SQL_ERROR);

  if (ret != SQL_SUCCESS) {
    // TODO remove later
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_STMT, statement) << std::endl;
  }

  VerifyOdbcErrorState(SQL_HANDLE_STMT, statement, error_code);
}

// Validate unsigned length SQLULEN return value
void validateSetStmtAttr(SQLHSTMT statement, SQLUSMALLINT attribute, SQLULEN new_value) {
  SQLINTEGER stringLengthPtr = sizeof(SQLULEN);

  SQLRETURN ret = SQLSetStmtAttr(statement, attribute, &new_value, stringLengthPtr);

  if (ret != SQL_SUCCESS) {
    // TODO remove later
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_STMT, statement) << std::endl;
  }

  EXPECT_EQ(ret, SQL_SUCCESS);
}

// Validate error return value and code
void validateSetStmtAttrErrorCode(SQLHSTMT statement, SQLUSMALLINT attribute,
                                  SQLULEN new_value, std::string_view error_code) {
  // SQLINTEGER stringLengthPtr;

  SQLRETURN ret = SQLSetStmtAttr(statement, attribute, &new_value, 0);

  if (ret != SQL_SUCCESS) {
    // TODO remove later
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_STMT, statement) << std::endl;
  }

  EXPECT_EQ(ret, SQL_ERROR);

  VerifyOdbcErrorState(SQL_HANDLE_STMT, statement, error_code);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrAppParamDesc) {
  this->connect();

  validateGetStmtAttrGreaterThan(this->stmt, SQL_ATTR_APP_PARAM_DESC,
                                 static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrAppRowDesc) {
  this->connect();

  validateGetStmtAttrGreaterThan(this->stmt, SQL_ATTR_APP_ROW_DESC,
                                 static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrAsyncEnable) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_ASYNC_ENABLE,
                      static_cast<SQLULEN>(SQL_ASYNC_ENABLE_OFF));

  this->disconnect();
}

// #ifdef SQL_ATTR_ASYNC_STMT_EVENT
//  case SQL_ATTR_ASYNC_STMT_EVENT:
//   throw DriverException("Unsupported attribute", "HYC00");
// #endif
//
#ifdef SQL_ATTR_ASYNC_STMT_EVENT
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrAsyncStmtEventUnsupported) {
  this->connect();

  // Optional feature not implemented
  validateGetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_EVENT, error_state_HYC00);

  this->disconnect();
}
#endif

// #ifdef SQL_ATTR_ASYNC_STMT_PCALLBACK
//  case SQL_ATTR_ASYNC_STMT_PCALLBACK:
//   throw DriverException("Unsupported attribute", "HYC00");
// #endif
//
#ifdef SQL_ATTR_ASYNC_STMT_PCALLBACK
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrAsyncStmtPCCallbackUnsupported) {
  this->connect();

  // Optional feature not implemented
  validateGetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_PCALLBACK,
                               error_state_HYC00);

  this->disconnect();
}
#endif

// #ifdef SQL_ATTR_ASYNC_STMT_PCONTEXT
//  case SQL_ATTR_ASYNC_STMT_PCONTEXT:
//   throw DriverException("Unsupported attribute", "HYC00");
// #endif
//
#ifdef SQL_ATTR_ASYNC_STMT_PCONTEXT
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrAsyncStmtPCContextUnsupported) {
  this->connect();

  // Optional feature not implemented
  validateGetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_PCONTEXT,
                               error_state_HYC00);

  this->disconnect();
}
#endif

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrConcurrency) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_CONCURRENCY,
                      static_cast<SQLULEN>(SQL_CONCUR_READ_ONLY));

  this->disconnect();
}

//  case SQL_ATTR_CURSOR_SCROLLABLE:
// GetAttribute(static_cast<SQLULEN>(SQL_NONSCROLLABLE), output, bufferSize, strLenPtr);
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrCursorScrollable) {
  // TODO HY092: [Apache Arrow][Flight SQL] (100) Invalid statement attribute: 65535
  GTEST_SKIP();
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_CURSOR_SCROLLABLE,
                      static_cast<SQLULEN>(SQL_NONSCROLLABLE));

  this->disconnect();
}

// case SQL_ATTR_CURSOR_SENSITIVITY:
//   GetAttribute(static_cast<SQLULEN>(SQL_UNSPECIFIED), output, bufferSize, strLenPtr);
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrCursorSensitivity) {
  // TODO HY092: [Apache Arrow][Flight SQL] (100) Invalid statement attribute: 65534
  GTEST_SKIP();
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_CURSOR_SENSITIVITY,
                      static_cast<SQLULEN>(SQL_UNSPECIFIED));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrCursorType) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_CURSOR_TYPE,
                      static_cast<SQLULEN>(SQL_CURSOR_FORWARD_ONLY));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrEnableAutoIPD) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_ENABLE_AUTO_IPD,
                      static_cast<SQLULEN>(SQL_FALSE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrFetchBookmarkPointer) {
  this->connect();

  // TODO static_cast<SQLPOINTER>(NULL) does not compile, but was used as default in
  // odbc_statement.cc
  // validateGetStmtAttr(this->stmt, SQL_ATTR_FETCH_BOOKMARK_PTR,
  // static_cast<SQLPOINTER>(NULL));
  validateGetStmtAttr(this->stmt, SQL_ATTR_FETCH_BOOKMARK_PTR, static_cast<SQLLEN>(NULL));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrIMPParamDesc) {
  this->connect();

  validateGetStmtAttrGreaterThan(this->stmt, SQL_ATTR_IMP_PARAM_DESC,
                                 static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrIMPRowDesc) {
  this->connect();

  validateGetStmtAttrGreaterThan(this->stmt, SQL_ATTR_IMP_ROW_DESC,
                                 static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrKeysetSize) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_KEYSET_SIZE, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrMaxLength) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_MAX_LENGTH, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrMaxRows) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_MAX_ROWS, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrMetadataID) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_METADATA_ID, static_cast<SQLULEN>(SQL_FALSE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrNoscan) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_NOSCAN, static_cast<SQLULEN>(SQL_NOSCAN_OFF));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrParamBindOffsetPtr) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR,
                      static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrParamBindType) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_TYPE,
                      static_cast<SQLULEN>(SQL_PARAM_BIND_BY_COLUMN));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrParamOperationPtr) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_OPERATION_PTR, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrParamStatusPtr) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_STATUS_PTR, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrParamsProcessedPtr) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrParamsetSize) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_PARAMSET_SIZE, static_cast<SQLULEN>(1));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrQueryTimeout) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_QUERY_TIMEOUT, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRetrieveData) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_RETRIEVE_DATA,
                      static_cast<SQLULEN>(SQL_RD_ON));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowArraySize) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_ROW_ARRAY_SIZE, static_cast<SQLULEN>(1));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowBindOffsetPtr) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_OFFSET_PTR, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowBindType) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_TYPE, static_cast<SQLULEN>(0));

  this->disconnect();
}

// An SQLULEN value that is the number of the current row in the entire result set.If the
//        number of the current row cannot be determined or
//    there is no current row,
//    the driver returns 0.
//
//    This attribute can be retrieved by a call to SQLGetStmtAttr but not set by a call to
//        SQLSetStmtAttr.
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowNumber) {
  // TODO 24000: [Microsoft][ODBC Driver Manager] Invalid cursor state
  GTEST_SKIP();
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_ROW_NUMBER, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowOperationPtr) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_ROW_OPERATION_PTR, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowStatusPtr) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_ROW_STATUS_PTR, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowsFetchedPtr) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_ROWS_FETCHED_PTR, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrSimulateCursor) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_SIMULATE_CURSOR,
                      static_cast<SQLULEN>(SQL_SC_UNIQUE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrUseBookmarks) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_USE_BOOKMARKS,
                      static_cast<SQLULEN>(SQL_UB_OFF));

  this->disconnect();
}

// TODO This is not a standard SQL_ATTR type parameter
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowsetSize) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ROWSET_SIZE, static_cast<SQLULEN>(1));

  this->disconnect();
}

// The handle to the APD for subsequent calls to SQLExecute and SQLExecDirect on the
// statement handle. The initial value of this attribute is the descriptor implicitly
// allocated when the statement was initially allocated. If the value of this attribute is
// set to SQL_NULL_DESC or the handle originally allocated for the descriptor, an
// explicitly allocated APD handle that was previously associated with the statement
// handle is dissociated from it and the statement handle reverts to the implicitly
// allocated APD handle.
//
// This attribute cannot be set to a descriptor handle that was implicitly allocated for
// another statement or to another descriptor handle that was implicitly set on the same
// statement; implicitly allocated descriptor handles cannot be associated with more than
// one
//        statement or
//    descriptor handle.
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrAppParamDesc) {
  // GTEST_SKIP();
  SQLULEN app_param_desc = 0;
  SQLINTEGER stringLengthPtr;
  this->connect();

  SQLRETURN ret = SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC, &app_param_desc, 0,
                                 &stringLengthPtr);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // TODO SQL_NULL_DESC not found
  // setStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC,
  // static_cast<SQLULEN>(SQL_NULL_DESC)); setStmtAttr(this->stmt,
  // SQL_ATTR_APP_PARAM_DESC, static_cast<SQLULEN>(0));

  // validateSetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC,
  //                     static_cast<SQLULEN>(app_param_desc));

  this->disconnect();
}

// The handle to the ARD for subsequent fetches on the statement handle. The initial value
// of this attribute is the descriptor implicitly allocated when the statement was
// initially allocated. If the value of this attribute is set to SQL_NULL_DESC or the
// handle originally allocated for the descriptor, an explicitly allocated ARD handle that
// was previously associated with the statement handle is dissociated from it and the
// statement handle reverts to the implicitly allocated ARD handle.
//
// This attribute cannot be set to a descriptor handle that was implicitly allocated for
// another statement or to another descriptor handle that was implicitly set on the same
// statement; implicitly allocated descriptor handles cannot be associated with more than
// one
//         statement or descriptor handle.
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrAppRowDesc) {
  // GTEST_SKIP();
  SQLULEN app_row_desc = 0;
  SQLINTEGER stringLengthPtr;
  this->connect();

  SQLRETURN ret = SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC, &app_row_desc, 0,
                                 &stringLengthPtr);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // TODO SQL_NULL_DESC not found
  // setStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC, static_cast<SQLULEN>(SQL_NULL_DESC));
  // setStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC, static_cast<SQLULEN>(0));

  // validateSetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC,
  //                     static_cast<SQLULEN>(app_row_desc));

  this->disconnect();
}

// case SQL_ATTR_ASYNC_ENABLE:
//   throw DriverException("Unsupported attribute", "HYC00");
//
#ifdef SQL_ATTR_ASYNC_ENABLE
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrAsyncEnableUnsupported) {
  // TODO HY024: [Microsoft][ODBC Driver Manager] Invalid argument value
  GTEST_SKIP();
  this->connect();

  // Optional feature not implemented
  validateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_ENABLE, SQL_ASYNC_ENABLE_OFF,
                               error_state_HYC00);

  this->disconnect();
}
#endif

// #ifdef SQL_ATTR_ASYNC_STMT_EVENT
//  case SQL_ATTR_ASYNC_STMT_EVENT:
//    throw DriverException("Unsupported attribute", "HYC00");
// #endif
//
#ifdef SQL_ATTR_ASYNC_STMT_EVENT
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrAsyncStmtEventUnsupported) {
  // TODO HY118: [Microsoft][ODBC Driver Manager] Driver does not support asynchronous
  // notification
  GTEST_SKIP();
  this->connect();

  // Optional feature not implemented
  validateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_EVENT, 0,
                               error_state_HYC00);

  this->disconnect();
}
#endif

// TODO
//
// Only the Driver Manager can call a driver's SQLSetStmtAttr function with this
// attribute.
//
// #ifdef SQL_ATTR_ASYNC_STMT_PCALLBACK
//  case SQL_ATTR_ASYNC_STMT_PCALLBACK:
//   throw DriverException("Unsupported attribute", "HYC00");
// #endif
//
#ifdef SQL_ATTR_ASYNC_STMT_PCALLBACK
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrAsyncStmtPCCallbackUnsupported) {
  this->connect();

  validateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_PCALLBACK, 0,
                               error_state_HYC00);

  this->disconnect();
}
#endif

// TODO
// Only the Driver Manager can call a driver's SQLSetStmtAttr function with this
// attribute.
//
// #ifdef SQL_ATTR_ASYNC_STMT_PCONTEXT
//  case SQL_ATTR_ASYNC_STMT_PCONTEXT:
//   throw DriverException("Unsupported attribute", "HYC00");
// #endif
//
#ifdef SQL_ATTR_ASYNC_STMT_PCONTEXT
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrAsyncStmtPCContextUnsupported) {
  this->connect();

  // Optional feature not implemented
  validateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_PCONTEXT, 0,
                               error_state_HYC00);

  this->disconnect();
}
#endif

// SQL_CONCUR_READ_ONLY = Cursor is read-only. No updates are allowed.
//
// SQL_CONCUR_LOCK = Cursor uses the lowest level of locking sufficient to ensure that the
// row can be updated.
//
// SQL_CONCUR_ROWVER = Cursor uses optimistic concurrency control, comparing row versions
// such as SQLBase ROWID or Sybase TIMESTAMP.
//
// SQL_CONCUR_VALUES = Cursor uses optimistic concurrency control, comparing values.
//
// The default value for SQL_ATTR_CONCURRENCY is SQL_CONCUR_READ_ONLY.
//
//     case SQL_ATTR_CONCURRENCY:
//  CheckIfAttributeIsSetToOnlyValidValue(value,
//  static_cast<SQLULEN>(SQL_CONCUR_READ_ONLY));
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrConcurrency) {
  // TODO HY024: [Microsoft][ODBC Driver Manager] Invalid argument value
  GTEST_SKIP();
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_CONCURRENCY,
                      static_cast<SQLULEN>(SQL_CONCUR_READ_ONLY));

  this->disconnect();
}

// SQL_NONSCROLLABLE = Scrollable cursors are not required on the statement handle.If the
//                        application calls SQLFetchScroll on this handle,
//    the only valid value of FetchOrientation is SQL_FETCH_NEXT.This is the default.
//
//    SQL_SCROLLABLE = Scrollable cursors are required on the statement handle.When
//    calling
//                         SQLFetchScroll,
//   the application may specify any valid value of FetchOrientation,
//   achieving cursor positioning in modes other than the sequential mode.
//
// case SQL_ATTR_CURSOR_SCROLLABLE:
//  CheckIfAttributeIsSetToOnlyValidValue(value, static_cast<SQLULEN>(SQL_NONSCROLLABLE));
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrCursorScrollable) {
  // TODO HY092: [Apache Arrow][Flight SQL] (100) Invalid attribute: 0
  GTEST_SKIP();
  this->connect();

  // Both 0 or 1 are
  validateSetStmtAttr(this->stmt, SQL_ATTR_CURSOR_SCROLLABLE,
                      static_cast<SQLULEN>(SQL_NONSCROLLABLE));
  // validateSetStmtAttr(this->stmt, SQL_ATTR_CURSOR_SCROLLABLE,
  //                     static_cast<SQLULEN>(SQL_SCROLLABLE));

  this->disconnect();
}

// SQL_UNSPECIFIED =
//    It is unspecified what the cursor type is and
//    whether cursors on the statement handle make visible the changes made to a result
//    set
//        by another cursor.Cursors on the statement handle may make visible none,
//    some,
//    or all such changes.This is the default.
//
//        SQL_INSENSITIVE =
//        All cursors on the statement handle show the result set without reflecting any
//            changes made to it by any other cursor.Insensitive cursors are read -
//        only.This corresponds to a static cursor,
//    which has a concurrency that is read - only.
//
//                                           SQL_SENSITIVE =
//        All cursors on the statement handle make visible all changes made to a result
//        set
//            by another cursor.
//
// case SQL_ATTR_CURSOR_SENSITIVITY:
//  CheckIfAttributeIsSetToOnlyValidValue(value, static_cast<SQLULEN>(SQL_UNSPECIFIED));
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrCursorSensitivity) {
  // TODO HY092: [Apache Arrow][Flight SQL] (100) Invalid attribute: 0
  GTEST_SKIP();
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_CURSOR_SENSITIVITY,
                      static_cast<SQLULEN>(SQL_UNSPECIFIED));

  this->disconnect();
}

// SQL_CURSOR_FORWARD_ONLY = The cursor only scrolls forward.
//
// SQL_CURSOR_STATIC = The data in the result set is static.
//
// SQL_CURSOR_KEYSET_DRIVEN = The driver saves and uses the keys for the number of rows
// specified in the SQL_ATTR_KEYSET_SIZE statement attribute.
//
// SQL_CURSOR_DYNAMIC = The driver saves and uses only the keys for the rows in the
// rowset.
//
// The default value is SQL_CURSOR_FORWARD_ONLY. This attribute cannot be specified after
// the SQL statement has been prepared.
//
//
// If the specified cursor type is not supported by the data source,
//  the driver substitutes a different cursor type and returns
//          SQLSTATE 01S02(Option value changed)
//              .For a mixed
//      or dynamic cursor,
//  the driver substitutes, in order,
//  a keyset - driven or static cursor.For a keyset - driven cursor,
//  the driver substitutes a static cursor.
//
// case SQL_ATTR_CURSOR_SENSITIVITY:
//  CheckIfAttributeIsSetToOnlyValidValue(value, static_cast<SQLULEN>(SQL_UNSPECIFIED));
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrCursorType) {
  // TODO HY024: [Microsoft][ODBC Driver Manager] Invalid argument value
  GTEST_SKIP();
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_CURSOR_TYPE,
                      static_cast<SQLULEN>(SQL_CURSOR_FORWARD_ONLY));

  this->disconnect();
}

// SQL_TRUE = Turns on automatic population of the IPD after a call to
// SQLPrepare.SQL_FALSE =
//    Turns off automatic population of the IPD after a call to SQLPrepare
//        .(An application can still obtain IPD field information by calling
//              SQLDescribeParam,
//          if supported.)The default value of the statement attribute
//            SQL_ATTR_ENABLE_AUTO_IPD is SQL_FALSE.
//
// case SQL_ATTR_ENABLE_AUTO_IPD:
//  CheckIfAttributeIsSetToOnlyValidValue(value, static_cast<SQLULEN>(SQL_FALSE));
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrEnableAutoIPD) {
  // TODO HY024: [Microsoft][ODBC Driver Manager] Invalid argument value
  GTEST_SKIP();
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_ENABLE_AUTO_IPD,
                      static_cast<SQLULEN>(SQL_FALSE));

  this->disconnect();
}

// A SQLLEN *that points to a binary bookmark value.When SQLFetchScroll is called with
//    fFetchOrientation equal to SQL_FETCH_BOOKMARK,
//    the driver picks up the bookmark value
//        from this field.This field defaults to a null pointer.For more information,
//    see Scrolling by Bookmark.
//
// case SQL_ATTR_FETCH_BOOKMARK_PTR:
//   if (value != NULL) {
//     throw DriverException("Optional feature not implemented", "HYC00");
//   }
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrFetchBookmarkPointer) {
  // TODO HYC00: [Apache Arrow][Flight SQL] (100) Optional feature not implemented
  GTEST_SKIP();
  this->connect();

  // TODO Passing NULL in does not get recognized as null
  validateSetStmtAttr(this->stmt, SQL_ATTR_FETCH_BOOKMARK_PTR, static_cast<SQLLEN>(NULL));

  this->disconnect();
}

// The handle to the
//    IPD.The value of this attribute is the descriptor allocated when the statement was
//        initially allocated.The application cannot set this attribute.
//
//    This attribute can be retrieved by a call to SQLGetStmtAttr but not set by a call to
//        SQLSetStmtAttr.
//
// case SQL_ATTR_IMP_PARAM_DESC:
//  throw DriverException("Cannot assign implementation descriptor.", "HY017");
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrIMPParamDesc) {
  // TODO HY017: [Microsoft][ODBC Driver Manager] Invalid use of an
  // automatically-allocated descriptor handle
  GTEST_SKIP();
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_IMP_PARAM_DESC, static_cast<SQLULEN>(0));

  this->disconnect();
}

// The handle to the
//    IRD.The value of this attribute is the descriptor allocated when the statement was
//        initially allocated.The application cannot set this attribute.
//
//    This attribute can be retrieved by a call to SQLGetStmtAttr but not set by a call to
//        SQLSetStmtAttr.
//
// case SQL_ATTR_IMP_ROW_DESC:
//  throw DriverException("Cannot assign implementation descriptor.", "HY017");
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrIMPRowDesc) {
  // TODO HY017: [Microsoft][ODBC Driver Manager] Invalid use of an
  // automatically-allocated descriptor handle
  GTEST_SKIP();
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_IMP_ROW_DESC, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrKeysetSizeUnsupported) {
  this->connect();

  // Optional feature not implemented
  validateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_KEYSET_SIZE, 0, error_state_HYC00);

  this->disconnect();
}

// An SQLULEN value that specifies the maximum amount of data that the driver returns from
// a
//        character or
//    binary column.If ValuePtr is less than the length of the available data,
//    SQLFetch or SQLGetData truncates the data and returns SQL_SUCCESS.If ValuePtr
//                    is 0(the default),
//    the driver attempts to return all available data.
//
//        If the specified length is less than the minimum amount of data that the data
//        source can return or
//        greater than the maximum amount of data that the data source can return,
//    the driver substitutes that value and returns SQLSTATE 01S02(Option value changed)
//        .
//
//    The value of this attribute can be set on an open cursor; however, the setting might
//    not take effect immediately, in which case the driver will return SQLSTATE 01S02
//    (Option value changed) and reset the attribute to its original value.
//
// This attribute is intended to reduce network traffic and should be supported only when
// the data source (as opposed to the driver) in a multiple-tier driver can implement it.
// This mechanism should not be used by applications to truncate data; to truncate data
// received, an application should specify the maximum buffer length in the BufferLength
// argument in SQLBindCol or SQLGetData.
//
// case SQL_ATTR_MAX_LENGTH:
//  SetAttribute(value, attributeToWrite);
//  successfully_written =
//      m_spiStatement->SetAttribute(Statement::MAX_LENGTH, attributeToWrite);
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrMaxLength) {
  // TODO 01S02: [Apache Arrow][Flight SQL] (1000000) Optional value changed.
  GTEST_SKIP();
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_MAX_LENGTH, static_cast<SQLULEN>(1));

  this->disconnect();
}

// An SQLULEN value corresponding to the maximum number of rows to return to the
// application for a SELECT statement. If *ValuePtr equals 0 (the default), the driver
// returns all rows.
//
// This attribute is intended to reduce network traffic. Conceptually, it is applied when
// the result set is created and limits the result set to the first ValuePtr rows. If the
// number of rows in the result set is greater than ValuePtr, the result set is truncated.
//
// SQL_ATTR_MAX_ROWS applies to all result sets on the Statement, including those returned
// by catalog functions. SQL_ATTR_MAX_ROWS establishes a maximum for the value of the
// cursor row count.
//
// A driver should not emulate SQL_ATTR_MAX_ROWS behavior for SQLFetch or SQLFetchScroll
// (if result set size limitations cannot be implemented at the data source) if it cannot
// guarantee that SQL_ATTR_MAX_ROWS will be implemented properly.
//
// It is driver-defined whether SQL_ATTR_MAX_ROWS applies to statements other than SELECT
// statements (such as catalog functions).
//
// The value of this attribute can be set on an open cursor; however, the setting might
// not take effect immediately, in which case the driver will return SQLSTATE 01S02
// (Option value changed) and reset the attribute to its original value.
//
//  case SQL_ATTR_MAX_ROWS:
//    throw DriverException("Cannot set read-only attribute", "HY092");
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrMaxRows) {
  this->connect();

  // TODO HY092: [Apache Arrow][Flight SQL] (100) Cannot set read-only attribute
  // validateSetStmtAttr(this->stmt, SQL_ATTR_MAX_ROWS, static_cast<SQLULEN>(0));
  //
  // Cannot set read-only attribute
  validateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_MAX_ROWS, static_cast<SQLULEN>(0),
                               error_state_HY092);

  this->disconnect();
}

// An SQLULEN value that determines how the string arguments of catalog functions are
// treated.
//
// If SQL_TRUE, the string argument of catalog functions are treated as identifiers. The
// case is not significant. For nondelimited strings, the driver removes any trailing
// spaces and the string is folded to uppercase. For delimited strings, the driver removes
// any leading or trailing spaces and takes whatever is between the delimiters literally.
// If one of these arguments is set to a null pointer, the function returns SQL_ERROR and
// SQLSTATE HY009 (Invalid use of null pointer).
//
// If SQL_FALSE, the string arguments of catalog functions are not treated as identifiers.
// The case is significant. They can either contain a string search pattern or not,
// depending on the argument.
//
// The default value is SQL_FALSE.
//
// The TableType argument of SQLTables, which takes a list of values, is not affected by
// this attribute.
//
// SQL_ATTR_METADATA_ID can also be set on the connection level. (It and
// SQL_ATTR_ASYNC_ENABLE are the only statement attributes that are also connection
// attributes.)
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrMetadataID) {
  // TODO HY024: [Microsoft][ODBC Driver Manager] Invalid argument value
  GTEST_SKIP();
  this->connect();

  // validateSetStmtAttr(this->stmt, SQL_ATTR_METADATA_ID,
  // static_cast<SQLULEN>(SQL_TRUE));
  validateSetStmtAttr(this->stmt, SQL_ATTR_METADATA_ID, static_cast<SQLULEN>(SQL_FALSE));

  this->disconnect();
}

// An SQLULEN value that indicates whether the driver should scan SQL strings for escape
// sequences:
//
// SQL_NOSCAN_OFF = The driver scans SQL strings for escape sequences (the default).
//
// SQL_NOSCAN_ON = The driver does not scan SQL strings for escape sequences. Instead, the
// driver sends the statement directly to the data source.
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrNoscan) {
  // TODO HY024: [Microsoft][ODBC Driver Manager] Invalid argument value
  GTEST_SKIP();
  this->connect();

  // validateSetStmtAttr(this->stmt, SQL_ATTR_NOSCAN,
  // static_cast<SQLULEN>(SQL_NOSCAN_ON));
  validateSetStmtAttr(this->stmt, SQL_ATTR_NOSCAN, static_cast<SQLULEN>(SQL_NOSCAN_OFF));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrParamBindOffsetPtr) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR,
                      static_cast<SQLULEN>(1));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrParamBindType) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_TYPE,
                      static_cast<SQLULEN>(SQL_PARAM_BIND_BY_COLUMN));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrParamOperationPtr) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_PARAM_OPERATION_PTR, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrParamStatusPtr) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_PARAM_STATUS_PTR, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrParamsProcessedPtr) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrParamsetSize) {
  // TODO HY090: [Microsoft][ODBC Driver Manager] Invalid string or buffer length
  GTEST_SKIP();
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_PARAMSET_SIZE, static_cast<SQLULEN>(1));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrQueryTimeout) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_QUERY_TIMEOUT, static_cast<SQLULEN>(1));

  this->disconnect();
}

// An SQLULEN value:
//
// SQL_RD_ON = SQLFetchScroll and, in ODBC 3.x, SQLFetch retrieve data after it positions
// the cursor to the specified location. This is the default.
//
// SQL_RD_OFF = SQLFetchScroll and, in ODBC 3.x, SQLFetch do not retrieve data after it
// positions the cursor.
//
// By setting SQL_RETRIEVE_DATA to SQL_RD_OFF, an application can verify that a row exists
// or retrieve a bookmark for the row without incurring the overhead of retrieving rows.
// For more information, see Scrolling and Fetching Rows.
//
// The value of this attribute can be set on an open cursor; however, the setting might
// not take effect immediately, in which case the driver will return SQLSTATE 01S02
// (Option value changed) and reset the attribute to its original value.
//
// case SQL_ATTR_RETRIEVE_DATA:
//  CheckIfAttributeIsSetToOnlyValidValue(value, static_cast<SQLULEN>(SQL_TRUE));
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRetrieveData) {
  // TODO HY024: [Microsoft][ODBC Driver Manager] Invalid argument value
  GTEST_SKIP();
  this->connect();

  // TODO
  // validateSetStmtAttr(this->stmt, SQL_ATTR_RETRIEVE_DATA,
  //                    static_cast<SQLULEN>(SQL_RD_OFF));
  // validateSetStmtAttr(this->stmt, SQL_ATTR_RETRIEVE_DATA,
  //                    static_cast<SQLULEN>(SQL_RD_OFF));
  validateSetStmtAttr(this->stmt, SQL_ATTR_RETRIEVE_DATA,
                      static_cast<SQLULEN>(SQL_RD_ON));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowArraySize) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_ROW_ARRAY_SIZE, static_cast<SQLULEN>(1));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowBindOffsetPtr) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_OFFSET_PTR, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowBindType) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_TYPE, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowNumber) {
  this->connect();

  // Cannot set read-only attribute
  validateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ROW_NUMBER, static_cast<SQLULEN>(0),
                               error_state_HY092);

  this->disconnect();
}

// An SQLUSMALLINT * value that points to an array of SQLUSMALLINT values used to ignore a
// row during a bulk operation using SQLSetPos. Each value is set to either
// SQL_ROW_PROCEED (for the row to be included in the bulk operation) or SQL_ROW_IGNORE
// (for the row to be excluded from the bulk operation). (Rows cannot be ignored by using
// this array during calls to SQLBulkOperations.)
//
// This statement attribute can be set to a null pointer, in which case the driver does
// not return row status values. This attribute can be set at any time, but the new value
// is not used until the next time SQLSetPos is called.
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowOperationPtr) {
  this->connect();

  // TODO Can be set to null pointer
  validateSetStmtAttr(this->stmt, SQL_ATTR_ROW_OPERATION_PTR, static_cast<SQLULEN>(0));

  this->disconnect();
}

// An SQLUSMALLINT * value that points to an array of SQLUSMALLINT values containing row
// status values after a call to SQLFetch or SQLFetchScroll. The array has as many
// elements as there are rows in the rowset.
//
// This statement attribute can be set to a null pointer, in which case the driver does
// not return row status values. This attribute can be set at any time, but the new value
// is not used until the next time SQLBulkOperations, SQLFetch, SQLFetchScroll, or
// SQLSetPos is called.
//
// For more information, see Number of Rows Fetched and Status.
//
// Setting this statement attribute sets the SQL_DESC_ARRAY_STATUS_PTR field in the IRD
// header.
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowStatusPtr) {
  this->connect();

  // TODO Can be set to null pointer
  validateSetStmtAttr(this->stmt, SQL_ATTR_ROW_STATUS_PTR, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowsFetchedPtr) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_ROWS_FETCHED_PTR, static_cast<SQLULEN>(0));

  this->disconnect();
}

// An SQLULEN value that specifies whether drivers that simulate positioned update
//    and delete statements guarantee that such statements affect only one single row.
//
//    To simulate positioned update and delete statements,
//    most drivers construct a searched UPDATE or
//        DELETE statement containing a WHERE clause that specifies the value of each
//        column
//            in the current row.Unless these columns make up a unique key,
//    such a statement can affect more than one row.
//
//    To guarantee that such statements affect only one row,
//    the driver determines the columns in a unique key and adds these columns to the
//    result
//        set.If an application guarantees that the columns in the result set make up a
//            unique key,
//    the driver is not required to do so.This may reduce execution time.
//
//    SQL_SC_NON_UNIQUE = The driver does not guarantee that simulated positioned update
//    or
//                        delete statements will affect only one row; it is the
//                        application's responsibility to do so. If a statement affects
//                        more than one row, SQLExecute, SQLExecDirect, or SQLSetPos
//                        returns SQLSTATE 01001 (Cursor operation conflict).
//
// SQL_SC_TRY_UNIQUE = The driver attempts to guarantee that simulated positioned update
// or delete statements affect only one row. The driver always executes such statements,
// even if they might affect more than one row, such as when there is no unique key. If a
// statement affects more than one row, SQLExecute, SQLExecDirect, or SQLSetPos returns
// SQLSTATE 01001 (Cursor operation conflict).
//
// SQL_SC_UNIQUE = The driver guarantees that simulated positioned update or delete
// statements affect only one row. If the driver cannot guarantee this for a given
// statement, SQLExecDirect or SQLPrepare returns an error.
//
// If the data source provides native SQL support for positioned update and delete
// statements and the driver does not simulate cursors, SQL_SUCCESS is returned when
// SQL_SC_UNIQUE is requested for SQL_SIMULATE_CURSOR. SQL_SUCCESS_WITH_INFO is returned
// if SQL_SC_TRY_UNIQUE or SQL_SC_NON_UNIQUE is requested. If the data source provides the
// SQL_SC_TRY_UNIQUE level of support and the driver does not, SQL_SUCCESS is returned for
// SQL_SC_TRY_UNIQUE and SQL_SUCCESS_WITH_INFO is returned for SQL_SC_NON_UNIQUE.
//
// If the specified cursor simulation type is not supported by the data source, the driver
// substitutes a different simulation type and returns SQLSTATE 01S02 (Option value
// changed). For SQL_SC_UNIQUE, the driver substitutes, in order, SQL_SC_TRY_UNIQUE or
// SQL_SC_NON_UNIQUE. For SQL_SC_TRY_UNIQUE, the driver substitutes SQL_SC_NON_UNIQUE.
//
// The default is SQL_SC_UNIQUE.
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrSimulateCursor) {
  // TODO HY024: [Microsoft][ODBC Driver Manager] Invalid argument value
  GTEST_SKIP();
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_SIMULATE_CURSOR,
                      static_cast<SQLULEN>(SQL_SC_UNIQUE));

  this->disconnect();
}

// An SQLULEN value that specifies whether an application will use bookmarks with a
// cursor:
//
// SQL_UB_OFF = Off (the default)
//
// SQL_UB_VARIABLE = An application will use bookmarks with a cursor, and the driver will
// provide variable-length bookmarks if they are supported. SQL_UB_FIXED is deprecated in
// ODBC 3.x. ODBC 3.x applications should always use variable-length bookmarks, even when
// working with ODBC 2.x drivers (which supported only 4-byte, fixed-length bookmarks).
// This is because a fixed-length bookmark is just a special case of a variable-length
// bookmark. When working with an ODBC 2.x driver, the Driver Manager maps SQL_UB_VARIABLE
// to SQL_UB_FIXED.
//
// To use bookmarks with a cursor, the application must specify this attribute with the
// SQL_UB_VARIABLE value before opening the cursor.
//
// case SQL_ATTR_USE_BOOKMARKS:
//  CheckIfAttributeIsSetToOnlyValidValue(value, static_cast<SQLULEN>(SQL_UB_OFF));
//
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrUseBookmarks) {
  // TODO HY024: [Microsoft][ODBC Driver Manager] Invalid argument value
  GTEST_SKIP();
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_USE_BOOKMARKS,
                      static_cast<SQLULEN>(SQL_UB_OFF));

  this->disconnect();
}

// TODO This is not a standard SQL_ATTR type parameter
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowsetSize) {
  // TODO HY090: [Microsoft][ODBC Driver Manager] Invalid string or buffer length
  GTEST_SKIP();
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ROWSET_SIZE, static_cast<SQLULEN>(1));

  this->disconnect();
}

}  // namespace arrow::flight::sql::odbc
