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

#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/utf8.h"

#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/example/sqlite_server.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/encoding_utils.h"

#ifdef _WIN32
#  include <windows.h>
#endif

#include <sql.h>
#include <sqltypes.h>
#include <sqlucode.h>

#include <type_traits>

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_connection.h"

// For DSN registration
#include "arrow/flight/sql/odbc/flight_sql/system_dsn.h"

#define TEST_CONNECT_STR "ARROW_FLIGHT_SQL_ODBC_CONN"
#define TEST_DSN "Apache Arrow Flight SQL Test DSN"

namespace arrow {
namespace flight {
namespace odbc {
namespace integration_tests {
using driver::odbcabstraction::Connection;

class FlightSQLODBCRemoteTestBase : public ::testing::Test {
 public:
  /// \brief Connect to Arrow Flight SQL server using connection string defined in
  /// environment variable "ARROW_FLIGHT_SQL_ODBC_CONN"
  void connect();
  /// \brief Connect to Arrow Flight SQL server using connection string
  void connectWithString(std::string connection_str);
  /// \brief Disconnect from server
  void disconnect();
  /// \brief Get connection string from environment variable "ARROW_FLIGHT_SQL_ODBC_CONN"
  std::string virtual getConnectionString();
  /// \brief Get invalid connection string based on connection string defined in
  /// environment variable "ARROW_FLIGHT_SQL_ODBC_CONN"
  std::string virtual getInvalidConnectionString();

  /** ODBC Environment. */
  SQLHENV env;

  /** ODBC Connect. */
  SQLHDBC conn;

  /** ODBC Statement. */
  SQLHSTMT stmt;

 protected:
  void SetUp() override;
};

class MockFlightSqlServerAuthHandler : public ServerAuthHandler {
 public:
  explicit MockFlightSqlServerAuthHandler(const std::string& token);
  ~MockFlightSqlServerAuthHandler() override;
  Status Authenticate(const ServerCallContext& context, ServerAuthSender* outgoing,
                      ServerAuthReader* incoming) override;
  Status IsValid(const ServerCallContext& context, const std::string& token,
                 std::string* peer_identity) override;

 private:
  std::string token_;
};

class FlightSQLODBCMockTestBase : public FlightSQLODBCRemoteTestBase {
  // Sets up a mock server for each test case
 public:
  /// \brief Get connection string for mock server
  std::string getConnectionString() override;
  /// \brief Get invalid connection string for mock server
  std::string getInvalidConnectionString() override;

  int port;

 protected:
  void SetUp() override;

  void TearDown() override;

 private:
  std::shared_ptr<arrow::flight::sql::example::SQLiteFlightSqlServer> server;
};

template <typename T>
class FlightSQLODBCTestBase : public T {
 public:
  using List = std::list<T>;
};

using TestTypes =
    ::testing::Types<FlightSQLODBCMockTestBase, FlightSQLODBCRemoteTestBase>;
TYPED_TEST_SUITE(FlightSQLODBCTestBase, TestTypes);

/** ODBC read buffer size. */
enum { ODBC_BUFFER_SIZE = 1024 };

/// Compare ConnPropertyMap, key value is case-insensitive
bool compareConnPropertyMap(Connection::ConnPropertyMap map1,
                            Connection::ConnPropertyMap map2);

/// Get error message from ODBC driver using SQLGetDiagRec
std::string GetOdbcErrorMessage(SQLSMALLINT handle_type, SQLHANDLE handle);

/// Verify ODBC Error State
void VerifyOdbcErrorState(SQLSMALLINT handle_type, SQLHANDLE handle,
                          std::string expected_state);

/// \brief Write connection string into DSN
/// \param[in] connection_str the connection string.
/// \return true on success
bool writeDSN(std::string connection_str);

/// \brief Write properties map into DSN
/// \param[in] properties map.
/// \return true on success
bool writeDSN(Connection::ConnPropertyMap properties);
}  // namespace integration_tests
}  // namespace odbc
}  // namespace flight
}  // namespace arrow
