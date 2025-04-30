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


#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_environment.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/connection.h>
#include <arrow/flight/sql/odbc/flight_sql/include/flight_sql/flight_sql_driver.h>

// odbc_api includes windows.h, which needs to be put behind winsock2.h.
// odbc_environment.h includes winsock2.h
#include <arrow/flight/sql/odbc/odbc_api.h>

namespace arrow
{
  SQLRETURN SQLAllocEnv(SQLHENV* env) {
    using driver::flight_sql::FlightSqlDriver;
    using ODBC::ODBCEnvironment;

    *env = SQL_NULL_HENV;

    std::shared_ptr<FlightSqlDriver> odbc_driver = std::make_shared<FlightSqlDriver>();
    *env = reinterpret_cast<SQLHENV>(new ODBCEnvironment(odbc_driver));

    return SQL_SUCCESS;
  }

  SQLRETURN SQLAllocConnect(SQLHENV env, SQLHDBC* conn) {
    using ODBC::ODBCConnection;
    using ODBC::ODBCEnvironment;

    *conn = SQL_NULL_HDBC;

    ODBCEnvironment* environment = reinterpret_cast<ODBCEnvironment*>(env);

    if (!environment) {
      return SQL_INVALID_HANDLE;
    }

    std::shared_ptr<ODBCConnection> connection = environment->CreateConnection();

    if (!connection) {
      return environment->GetDiagnostics().GetNativeError(0);
    }

    *connection = reinterpret_cast<SQLHANDLE>(&connection);

    return SQL_SUCCESS;
  }

  SQLRETURN SQLFreeEnv(SQLHENV env) {
    using ODBC::ODBCEnvironment;

    ODBCEnvironment* environment = reinterpret_cast<ODBCEnvironment*>(env);

    if (!environment) {
      return SQL_INVALID_HANDLE;
    }

    delete environment;

    return SQL_SUCCESS;
  }

  SQLRETURN SQLFreeConnect(SQLHDBC conn) {
    using ODBC::ODBCConnection;

    ODBCConnection* odbc_conn = reinterpret_cast<ODBCConnection*>(conn);

    if (!odbc_conn) {
      return SQL_INVALID_HANDLE;
    }

    // TODO: Fix Pointer or Reference to an incomplete type
    // odbc_connection->releaseConnection();

    delete odbc_conn;

    return SQL_SUCCESS;
  }

  }  // namespace arrow
