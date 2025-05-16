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

// For DSN registration. flight_sql_connection.h needs to included first due to conflicts
// with windows.h
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_connection.h"

#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/encoding_utils.h>

#include <arrow/flight/sql/odbc/tests/odbc_test_suite.h>

// For DSN registration
#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/config/configuration.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_connection.h"

namespace arrow {
namespace flight {
namespace odbc {
namespace integration_tests {
// -AL- can use environment variable for the connection string directly.

std::string GetOdbcErrorMessage(SQLSMALLINT handle_type, SQLHANDLE handle) {
  using ODBC::SqlStringToString;

  SQLCHAR sql_state[7] = {};
  SQLINTEGER native_code;

  SQLCHAR message[ODBC_BUFFER_SIZE] = {};
  SQLSMALLINT reallen = 0;

  // On Windows, reallen is in bytes. On Linux, reallen is in chars.
  // So, not using reallen
  SQLGetDiagRec(handle_type, handle, 1, sql_state, &native_code, message,
                ODBC_BUFFER_SIZE, &reallen);

  std::string res = SqlStringToString(sql_state);

  if (res.empty() || !message[0]) {
    res = "Cannot find ODBC error message";
  } else {
    res.append(": ").append(SqlStringToString(message));
  }

  return res;
}

void writeDSN(std::string connection_str) {
  // -AL- todo implement and add header to odbc_test_suite.h!!
  using driver::flight_sql::FlightSqlConnection;
  using driver::flight_sql::config::Configuration;
  using driver::odbcabstraction::Connection;
  using ODBC::ODBCConnection;

  Connection::ConnPropertyMap properties;

  ODBC::ODBCConnection::getPropertiesFromConnString(connection_str, properties);

  Configuration config;
  config.Set(FlightSqlConnection::DSN, std::string("Apache Arrow Flight SQL Test DSN"));
}

// -AL- todo potentially add `connectToFlightSql` here.
}  // namespace integration_tests
}  // namespace odbc
}  // namespace flight
}  // namespace arrow
