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

#ifdef _WIN32
#  include <windows.h>
#endif

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>
#include <sqlucode.h>

#include "arrow/flight/sql/odbc/odbc_api.h"
#include "arrow/flight/sql/odbc/visibility.h"

SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent,
  SQLHANDLE* result) {
  return arrow::SQLAllocHandle(type, parent, result);
}

SQLRETURN SQL_API SQLAllocEnv(SQLHENV* env) {
  return arrow::SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, env);
}

SQLRETURN SQL_API SQLDriverConnect(SQLHDBC conn, SQLHWND windowHandle,
  SQLWCHAR* inConnectionString,
  SQLSMALLINT inConnectionStringLen,
  SQLWCHAR* outConnectionString,
  SQLSMALLINT outConnectionStringBufferLen,
  SQLSMALLINT* outConnectionStringLen,
  SQLUSMALLINT driverCompletion) {
  // TODO: implement SQLDriverConnect by linking to `odbc_impl` //-AL- TODO: create GitHub issue for SQLDriverConnect implementation
  return SQL_INVALID_HANDLE;
}
