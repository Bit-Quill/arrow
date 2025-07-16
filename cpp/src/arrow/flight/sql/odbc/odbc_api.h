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

#pragma once

#ifdef _WIN32
#  include <windows.h>
#endif

#include <sql.h>
#include <sqltypes.h>
#include <sqlucode.h>

//  @file odbc_api.h
//
//  Define internal ODBC API function headers.
namespace arrow {
SQLRETURN SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE* result);
SQLRETURN SQLFreeHandle(SQLSMALLINT type, SQLHANDLE handle);
SQLRETURN SQLFreeStmt(SQLHSTMT stmt, SQLUSMALLINT option);
SQLRETURN SQLGetDiagField(SQLSMALLINT handleType, SQLHANDLE handle, SQLSMALLINT recNumber,
                          SQLSMALLINT diagIdentifier, SQLPOINTER diagInfoPtr,
                          SQLSMALLINT bufferLength, SQLSMALLINT* stringLengthPtr);
SQLRETURN SQLGetDiagRec(SQLSMALLINT handleType, SQLHANDLE handle, SQLSMALLINT recNumber,
                        SQLWCHAR* sqlState, SQLINTEGER* nativeErrorPtr,
                        SQLWCHAR* messageText, SQLSMALLINT bufferLength,
                        SQLSMALLINT* textLengthPtr);
SQLRETURN SQLGetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER valuePtr,
                        SQLINTEGER bufferLen, SQLINTEGER* strLenPtr);
SQLRETURN SQLSetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER valuePtr,
                        SQLINTEGER strLen);
SQLRETURN SQLGetConnectAttr(SQLHDBC conn, SQLINTEGER attribute, SQLPOINTER valuePtr,
                            SQLINTEGER bufferLength, SQLINTEGER* stringLengthPtr);
SQLRETURN SQLSetConnectAttr(SQLHDBC conn, SQLINTEGER attr, SQLPOINTER value,
                            SQLINTEGER valueLen);
SQLRETURN SQLDriverConnect(SQLHDBC conn, SQLHWND windowHandle,
                           SQLWCHAR* inConnectionString,
                           SQLSMALLINT inConnectionStringLen,
                           SQLWCHAR* outConnectionString,
                           SQLSMALLINT outConnectionStringBufferLen,
                           SQLSMALLINT* outConnectionStringLen,
                           SQLUSMALLINT driverCompletion);
SQLRETURN SQLConnect(SQLHDBC conn, SQLWCHAR* dsnName, SQLSMALLINT dsnNameLen,
                     SQLWCHAR* userName, SQLSMALLINT userNameLen, SQLWCHAR* password,
                     SQLSMALLINT passwordLen);
SQLRETURN SQLDisconnect(SQLHDBC conn);
SQLRETURN SQLGetInfo(SQLHDBC conn, SQLUSMALLINT infoType, SQLPOINTER infoValuePtr,
                     SQLSMALLINT bufLen, SQLSMALLINT* length);
SQLRETURN SQLGetStmtAttr(SQLHSTMT stmt, SQLINTEGER attribute, SQLPOINTER valuePtr,
                         SQLINTEGER bufferLength, SQLINTEGER* stringLengthPtr);
SQLRETURN SQLSetStmtAttr(SQLHSTMT stmt, SQLINTEGER attribute, SQLPOINTER valuePtr,
                         SQLINTEGER stringLength);
SQLRETURN SQLExecDirect(SQLHSTMT stmt, SQLWCHAR* queryText, SQLINTEGER textLength);
SQLRETURN SQLPrepare(SQLHSTMT stmt, SQLWCHAR* queryText, SQLINTEGER textLength);
SQLRETURN SQLExecute(SQLHSTMT stmt);
SQLRETURN SQLFetch(SQLHSTMT stmt);
SQLRETURN SQLFetchScroll(SQLHSTMT stmt, SQLSMALLINT fetchOrientation, SQLLEN fetchOffset);
SQLRETURN SQLBindCol(SQLHSTMT stmt, SQLUSMALLINT recordNumber, SQLSMALLINT cType,
                     SQLPOINTER dataPtr, SQLLEN bufferLength, SQLLEN* indicatorPtr);
SQLRETURN SQLGetData(SQLHSTMT stmt, SQLUSMALLINT recordNumber, SQLSMALLINT cType,
                     SQLPOINTER dataPtr, SQLLEN bufferLength, SQLLEN* indicatorPtr);
SQLRETURN SQLMoreResults(SQLHSTMT stmt);
SQLRETURN SQLNumResultCols(SQLHSTMT stmt, SQLSMALLINT* columnCountPtr);
SQLRETURN SQLRowCount(SQLHSTMT stmt, SQLLEN* rowCountPtr);
SQLRETURN SQLTables(SQLHSTMT stmt, SQLWCHAR* catalogName, SQLSMALLINT catalogNameLength,
                    SQLWCHAR* schemaName, SQLSMALLINT schemaNameLength,
                    SQLWCHAR* tableName, SQLSMALLINT tableNameLength, SQLWCHAR* tableType,
                    SQLSMALLINT tableTypeLength);
}  // namespace arrow
