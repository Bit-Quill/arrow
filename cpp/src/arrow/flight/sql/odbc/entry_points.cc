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

// platform.h includes windows.h, so it needs to be included first
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"

#ifdef _WIN32
#  include <windows.h>
#endif

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <sqlucode.h>

#include "arrow/flight/sql/odbc/odbc_api_internal.h"
#include "arrow/flight/sql/odbc/visibility.h"

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_connection.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_descriptor.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_environment.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_statement.h"

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/logger.h"

SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE* result) {
  return arrow::flight::sql::odbc::SQLAllocHandle(type, parent, result);
}

SQLRETURN SQL_API SQLAllocEnv(SQLHENV* env) {
  return arrow::flight::sql::odbc::SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, env);
}

SQLRETURN SQL_API SQLAllocConnect(SQLHENV env, SQLHDBC* conn) {
  return arrow::flight::sql::odbc::SQLAllocHandle(SQL_HANDLE_DBC, env, conn);
}

SQLRETURN SQL_API SQLAllocStmt(SQLHDBC conn, SQLHSTMT* stmt) {
  return arrow::flight::sql::odbc::SQLAllocHandle(SQL_HANDLE_STMT, conn, stmt);
}

SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT type, SQLHANDLE handle) {
  return arrow::flight::sql::odbc::SQLFreeHandle(type, handle);
}

SQLRETURN SQL_API SQLFreeEnv(SQLHENV env) {
  return arrow::flight::sql::odbc::SQLFreeHandle(SQL_HANDLE_ENV, env);
}

SQLRETURN SQL_API SQLFreeConnect(SQLHDBC conn) {
  return arrow::flight::sql::odbc::SQLFreeHandle(SQL_HANDLE_DBC, conn);
}

SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT stmt, SQLUSMALLINT option) {
  return arrow::flight::sql::odbc::SQLFreeStmt(stmt, option);
}

SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT handleType, SQLHANDLE handle,
                                  SQLSMALLINT recNumber, SQLSMALLINT diagIdentifier,
                                  SQLPOINTER diagInfoPtr, SQLSMALLINT bufferLength,
                                  SQLSMALLINT* stringLengthPtr) {
  return arrow::flight::sql::odbc::SQLGetDiagField(handleType, handle, recNumber,
                                                   diagIdentifier, diagInfoPtr,
                                                   bufferLength, stringLengthPtr);
}

SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT handleType, SQLHANDLE handle,
                                SQLSMALLINT recNumber, SQLWCHAR* sqlState,
                                SQLINTEGER* nativeErrorPtr, SQLWCHAR* messageText,
                                SQLSMALLINT bufferLength, SQLSMALLINT* textLengthPtr) {
  return arrow::flight::sql::odbc::SQLGetDiagRec(handleType, handle, recNumber, sqlState,
                                                 nativeErrorPtr, messageText,
                                                 bufferLength, textLengthPtr);
}

SQLRETURN SQL_API SQLGetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER valuePtr,
                                SQLINTEGER bufferLen, SQLINTEGER* strLenPtr) {
  return arrow::flight::sql::odbc::SQLGetEnvAttr(env, attr, valuePtr, bufferLen,
                                                 strLenPtr);
}

SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER valuePtr,
                                SQLINTEGER strLen) {
  return arrow::flight::sql::odbc::SQLSetEnvAttr(env, attr, valuePtr, strLen);
}

SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC conn, SQLINTEGER attribute,
                                    SQLPOINTER valuePtr, SQLINTEGER bufferLength,
                                    SQLINTEGER* stringLengthPtr) {
  return arrow::flight::sql::odbc::SQLGetConnectAttr(conn, attribute, valuePtr,
                                                     bufferLength, stringLengthPtr);
}

SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC conn, SQLINTEGER attr, SQLPOINTER value,
                                    SQLINTEGER valueLen) {
  return arrow::flight::sql::odbc::SQLSetConnectAttr(conn, attr, value, valueLen);
}

SQLRETURN SQL_API SQLGetInfo(SQLHDBC conn, SQLUSMALLINT infoType, SQLPOINTER infoValuePtr,
                             SQLSMALLINT bufLen, SQLSMALLINT* length) {
  return arrow::flight::sql::odbc::SQLGetInfo(conn, infoType, infoValuePtr, bufLen,
                                              length);
}

SQLRETURN SQL_API SQLDriverConnect(SQLHDBC conn, SQLHWND windowHandle,
                                   SQLWCHAR* inConnectionString,
                                   SQLSMALLINT inConnectionStringLen,
                                   SQLWCHAR* outConnectionString,
                                   SQLSMALLINT outConnectionStringBufferLen,
                                   SQLSMALLINT* outConnectionStringLen,
                                   SQLUSMALLINT driverCompletion) {
  return arrow::flight::sql::odbc::SQLDriverConnect(
      conn, windowHandle, inConnectionString, inConnectionStringLen, outConnectionString,
      outConnectionStringBufferLen, outConnectionStringLen, driverCompletion);
}

SQLRETURN SQL_API SQLConnect(SQLHDBC conn, SQLWCHAR* dsnName, SQLSMALLINT dsnNameLen,
                             SQLWCHAR* userName, SQLSMALLINT userNameLen,
                             SQLWCHAR* password, SQLSMALLINT passwordLen) {
  return arrow::flight::sql::odbc::SQLConnect(conn, dsnName, dsnNameLen, userName,
                                              userNameLen, password, passwordLen);
}

SQLRETURN SQL_API SQLDisconnect(SQLHDBC conn) {
  return arrow::flight::sql::odbc::SQLDisconnect(conn);
}

SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT stmt, SQLINTEGER attribute, SQLPOINTER valuePtr,
                                 SQLINTEGER bufferLength, SQLINTEGER* stringLengthPtr) {
  return arrow::flight::sql::odbc::SQLGetStmtAttr(stmt, attribute, valuePtr, bufferLength,
                                                  stringLengthPtr);
}

SQLRETURN SQL_API SQLExecDirect(SQLHSTMT stmt, SQLWCHAR* queryText,
                                SQLINTEGER textLength) {
  return arrow::flight::sql::odbc::SQLExecDirect(stmt, queryText, textLength);
}

SQLRETURN SQL_API SQLFetch(SQLHSTMT stmt) {
  return arrow::flight::sql::odbc::SQLFetch(stmt);
}

SQLRETURN SQL_API SQLExtendedFetch(SQLHSTMT stmt, SQLUSMALLINT fetchOrientation,
                                   SQLLEN fetchOffset, SQLULEN* rowCountPtr,
                                   SQLUSMALLINT* rowStatusArray) {
  return arrow::flight::sql::odbc::SQLExtendedFetch(stmt, fetchOrientation, fetchOffset,
                                                    rowCountPtr, rowStatusArray);
}

SQLRETURN SQL_API SQLFetchScroll(SQLHSTMT stmt, SQLSMALLINT fetchOrientation,
                                 SQLLEN fetchOffset) {
  return arrow::flight::sql::odbc::SQLFetchScroll(stmt, fetchOrientation, fetchOffset);
}

SQLRETURN SQL_API SQLGetData(SQLHSTMT stmt, SQLUSMALLINT recordNumber, SQLSMALLINT cType,
                             SQLPOINTER dataPtr, SQLLEN bufferLength,
                             SQLLEN* indicatorPtr) {
  return arrow::flight::sql::odbc::SQLGetData(stmt, recordNumber, cType, dataPtr,
                                              bufferLength, indicatorPtr);
}

SQLRETURN SQL_API SQLPrepare(SQLHSTMT stmt, SQLWCHAR* queryText, SQLINTEGER textLength) {
  return arrow::flight::sql::odbc::SQLPrepare(stmt, queryText, textLength);
}

SQLRETURN SQL_API SQLExecute(SQLHSTMT stmt) {
  return arrow::flight::sql::odbc::SQLExecute(stmt);
}

SQLRETURN SQL_API SQLBindCol(SQLHSTMT stmt, SQLUSMALLINT recordNumber, SQLSMALLINT cType,
                             SQLPOINTER dataPtr, SQLLEN bufferLength,
                             SQLLEN* indicatorPtr) {
  return arrow::flight::sql::odbc::SQLBindCol(stmt, recordNumber, cType, dataPtr,
                                              bufferLength, indicatorPtr);
}

SQLRETURN SQL_API SQLCancel(SQLHSTMT stmt) {
  LOG_DEBUG("SQLCancel called with stmt: {}", stmt);
  return ODBC::ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    throw driver::odbcabstraction::DriverException("SQLCancel is not implemented",
                                                   "IM001");
    return SQL_ERROR;
  });
}

SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT stmt) {
  return arrow::flight::sql::odbc::SQLCloseCursor(stmt);
}

SQLRETURN SQL_API SQLColAttribute(SQLHSTMT stmt, SQLUSMALLINT recordNumber,
                                  SQLUSMALLINT fieldIdentifier,
                                  SQLPOINTER characterAttributePtr,
                                  SQLSMALLINT bufferLength, SQLSMALLINT* outputLength,
                                  SQLLEN* numericAttributePtr) {
  return arrow::flight::sql::odbc::SQLColAttribute(stmt, recordNumber, fieldIdentifier,
                                                   characterAttributePtr, bufferLength,
                                                   outputLength, numericAttributePtr);
}

SQLRETURN SQL_API SQLTables(SQLHSTMT stmt, SQLWCHAR* catalogName,
                            SQLSMALLINT catalogNameLength, SQLWCHAR* schemaName,
                            SQLSMALLINT schemaNameLength, SQLWCHAR* tableName,
                            SQLSMALLINT tableNameLength, SQLWCHAR* tableType,
                            SQLSMALLINT tableTypeLength) {
  return arrow::flight::sql::odbc::SQLTables(stmt, catalogName, catalogNameLength,
                                             schemaName, schemaNameLength, tableName,
                                             tableNameLength, tableType, tableTypeLength);
}

SQLRETURN SQL_API SQLColumns(SQLHSTMT stmt, SQLWCHAR* catalogName,
                             SQLSMALLINT catalogNameLength, SQLWCHAR* schemaName,
                             SQLSMALLINT schemaNameLength, SQLWCHAR* tableName,
                             SQLSMALLINT tableNameLength, SQLWCHAR* columnName,
                             SQLSMALLINT columnNameLength) {
  return arrow::flight::sql::odbc::SQLColumns(
      stmt, catalogName, catalogNameLength, schemaName, schemaNameLength, tableName,
      tableNameLength, columnName, columnNameLength);
}

SQLRETURN SQL_API SQLForeignKeys(SQLHSTMT stmt, SQLWCHAR* pKCatalogName,
                                 SQLSMALLINT pKCatalogNameLength, SQLWCHAR* pKSchemaName,
                                 SQLSMALLINT pKSchemaNameLength, SQLWCHAR* pKTableName,
                                 SQLSMALLINT pKTableNameLength, SQLWCHAR* fKCatalogName,
                                 SQLSMALLINT fKCatalogNameLength, SQLWCHAR* fKSchemaName,
                                 SQLSMALLINT fKSchemaNameLength, SQLWCHAR* fKTableName,
                                 SQLSMALLINT fKTableNameLength) {
  LOG_DEBUG(
      "SQLForeignKeysW called with stmt: {}, pKCatalogName: {}, "
      "pKCatalogNameLength: "
      "{}, pKSchemaName: {}, pKSchemaNameLength: {}, pKTableName: {}, pKTableNameLength: "
      "{}, "
      "fKCatalogName: {}, fKCatalogNameLength: {}, fKSchemaName: {}, fKSchemaNameLength: "
      "{}, "
      "fKTableName: {}, fKTableNameLength : {}",
      stmt, fmt::ptr(pKCatalogName), pKCatalogNameLength, fmt::ptr(pKSchemaName),
      pKSchemaNameLength, fmt::ptr(pKTableName), pKTableNameLength,
      fmt::ptr(fKCatalogName), fKCatalogNameLength, fmt::ptr(fKSchemaName),
      fKSchemaNameLength, fmt::ptr(fKTableName), fKTableNameLength);
  return ODBC::ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    throw driver::odbcabstraction::DriverException("SQLForeignKeysW is not implemented",
                                                   "IM001");
    return SQL_ERROR;
  });
}

SQLRETURN SQL_API SQLGetTypeInfo(SQLHSTMT stmt, SQLSMALLINT dataType) {
  return arrow::flight::sql::odbc::SQLGetTypeInfo(stmt, dataType);
}

SQLRETURN SQL_API SQLMoreResults(SQLHSTMT stmt) {
  return arrow::flight::sql::odbc::SQLMoreResults(stmt);
}

SQLRETURN SQL_API SQLNativeSql(SQLHDBC connectionHandle, SQLWCHAR* inStatementText,
                               SQLINTEGER inStatementTextLength,
                               SQLWCHAR* outStatementText, SQLINTEGER bufferLength,
                               SQLINTEGER* outStatementTextLength) {
  return arrow::flight::sql::odbc::SQLNativeSql(connectionHandle, inStatementText,
                                                inStatementTextLength, outStatementText,
                                                bufferLength, outStatementTextLength);
}

SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT stmt, SQLSMALLINT* columnCountPtr) {
  return arrow::flight::sql::odbc::SQLNumResultCols(stmt, columnCountPtr);
}

SQLRETURN SQL_API SQLRowCount(SQLHSTMT stmt, SQLLEN* rowCountPtr) {
  return arrow::flight::sql::odbc::SQLRowCount(stmt, rowCountPtr);
}

SQLRETURN SQL_API SQLPrimaryKeys(SQLHSTMT stmt, SQLWCHAR* catalogName,
                                 SQLSMALLINT catalogNameLength, SQLWCHAR* schemaName,
                                 SQLSMALLINT schemaNameLength, SQLWCHAR* tableName,
                                 SQLSMALLINT tableNameLength) {
  LOG_DEBUG(
      "SQLPrimaryKeysW called with stmt: {}, catalogName: {}, "
      "catalogNameLength: "
      "{}, schemaName: {}, schemaNameLength: {}, tableName: {}, tableNameLength: {}",
      stmt, fmt::ptr(catalogName), catalogNameLength, fmt::ptr(schemaName),
      schemaNameLength, fmt::ptr(tableName), tableNameLength);
  return ODBC::ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    throw driver::odbcabstraction::DriverException("SQLPrimaryKeysW is not implemented",
                                                   "IM001");
    return SQL_ERROR;
  });
}

SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT stmt, SQLINTEGER attribute, SQLPOINTER valuePtr,
                                 SQLINTEGER stringLength) {
  return arrow::flight::sql::odbc::SQLSetStmtAttr(stmt, attribute, valuePtr,
                                                  stringLength);
}

SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT statementHandle, SQLUSMALLINT columnNumber,
                                 SQLWCHAR* columnName, SQLSMALLINT bufferLength,
                                 SQLSMALLINT* nameLengthPtr, SQLSMALLINT* dataTypePtr,
                                 SQLULEN* columnSizePtr, SQLSMALLINT* decimalDigitsPtr,
                                 SQLSMALLINT* nullablePtr) {
  return arrow::flight::sql::odbc::SQLDescribeCol(
      statementHandle, columnNumber, columnName, bufferLength, nameLengthPtr, dataTypePtr,
      columnSizePtr, decimalDigitsPtr, nullablePtr);
}
