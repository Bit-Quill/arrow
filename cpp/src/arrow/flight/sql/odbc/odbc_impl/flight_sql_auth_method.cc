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

#include "arrow/flight/sql/odbc/odbc_impl/flight_sql_auth_method.h"

#include "arrow/flight/client.h"
#include "arrow/flight/sql/odbc/odbc_impl/exceptions.h"
#include "arrow/flight/sql/odbc/odbc_impl/flight_sql_connection.h"
#include "arrow/flight/sql/odbc/odbc_impl/platform.h"
#include "arrow/result.h"
#include "arrow/status.h"

#include "arrow/util/logging.h" // -AL- TEMP

#include <optional>
#include <utility>

namespace arrow::flight::sql::odbc {

using arrow::Result;

namespace {
class NoOpAuthMethod : public FlightSqlAuthMethod {
 public:
  void Authenticate(FlightSqlConnection& connection,
                    FlightCallOptions& call_options) override {
    // Do nothing

    // GH-46733 TODO: implement NoOpAuthMethod to validate server address.
    // Can use NoOpClientAuthHandler.
    ARROW_LOG(DEBUG) << "NoOpAuthMethod::Authenticate 1"; // -AL- TEMP
  }
};

class NoOpClientAuthHandler : public ClientAuthHandler {
 public:
  std::string handshake_msg;

  NoOpClientAuthHandler() {}

  // -AL- todo: write class read/write auth handler for our mock server.
  // I think it is needed for stable behavior.
  Status Authenticate(ClientAuthSender* outgoing, ClientAuthReader* incoming) override {
    // The server should ignore this and just accept any Handshake
    // request. Some servers do not allow authentication with no handshakes.

    // -AL- the segfault is inside `TokenAuthMethod::Authenticate`. Ignore this func.
    ARROW_LOG(DEBUG) << "NoOpClientAuthHandler::Authenticate 1"; // -AL- TEMP
    ARROW_LOG(DEBUG) << "NoOpClientAuthHandler::Authenticate 2, outgoing: " << outgoing << ", incoming: " << incoming; // -AL- TEMP  
    
    if (!outgoing) { // -AL- TEMP, outgoing shouldn't be null.
        ARROW_LOG(ERROR) << "-AL- outgoing is null!";
        return Status::Invalid("Outgoing is null");
    }
    ARROW_LOG(DEBUG) << "NoOpClientAuthHandler::Authenticate 3 Write to server"; // -AL- TEMP
    // Status stat = outgoing->Write(std::string(" ")); // <- -AL- adding " " doesn't resolve segfault issue.
    // Status stat = Status::OK();
    // Status stat = outgoing->Write(std::string()); 
    handshake_msg = "handshake message";
    RETURN_NOT_OK(outgoing->Write(handshake_msg));
    ARROW_LOG(DEBUG) << "NoOpClientAuthHandler::Authenticate 4"; // -AL- TEMP  
    
    // -AL- must not read response from server as it is not guaranteed 
    return Status::OK(); // -AL- returning OK without write doesn't resolve the issue.
  }

  Status GetToken(std::string* token) override {
    ARROW_LOG(DEBUG) << "NoOpClientAuthHandler::GetToken 1"; // -AL- TEMP  
    *token = std::string();
    ARROW_LOG(DEBUG) << "NoOpClientAuthHandler::GetToken 2"; // -AL- TEMP  
    return Status::OK();
  }
};

class UserPasswordAuthMethod : public FlightSqlAuthMethod {
 public:
  UserPasswordAuthMethod(FlightClient& client, std::string user, std::string password)
      : client_(client), user_(std::move(user)), password_(std::move(password)) {}

  void Authenticate(FlightSqlConnection& connection,
                    FlightCallOptions& call_options) override {
    FlightCallOptions auth_call_options;
    const std::optional<Connection::Attribute>& login_timeout =
        connection.GetAttribute(Connection::LOGIN_TIMEOUT);
    if (login_timeout && std::get<uint32_t>(*login_timeout) > 0) {
      // ODBC's LOGIN_TIMEOUT attribute and FlightCallOptions.timeout use
      // seconds as time unit.
      double timeout_seconds = static_cast<double>(std::get<uint32_t>(*login_timeout));
      if (timeout_seconds > 0) {
        auth_call_options.timeout = TimeoutDuration{timeout_seconds};
      }
    }

    Result<std::pair<std::string, std::string>> bearer_result =
        client_.AuthenticateBasicToken(auth_call_options, user_, password_);

    if (!bearer_result.ok()) {
      const auto& flight_status =
          FlightStatusDetail::UnwrapStatus(bearer_result.status());
      if (flight_status != nullptr) {
        if (flight_status->code() == FlightStatusCode::Unauthenticated) {
          throw AuthenticationException(
              "Failed to authenticate with user and password: " +
              bearer_result.status().ToString());
        } else if (flight_status->code() == FlightStatusCode::Unavailable) {
          throw CommunicationException(bearer_result.status().message());
        }
      }

      throw DriverException(bearer_result.status().message());
    }

    // call_options may have already been populated with data from the connection string
    // or DSN. Ensure auth-generated headers are placed at the front of the header list.
    call_options.headers.insert(call_options.headers.begin(), bearer_result.ValueOrDie());
  }

  std::string GetUser() override { return user_; }

 private:
  FlightClient& client_;
  std::string user_;
  std::string password_;
};

class TokenAuthMethod : public FlightSqlAuthMethod {
 private:
  FlightClient& client_;
  std::string token_;  // this is the token the user provides

 public:
  TokenAuthMethod(FlightClient& client, std::string token)
      : client_{client}, token_{std::move(token)} {}

  void Authenticate(FlightSqlConnection& connection,
                    FlightCallOptions& call_options) override {
    ARROW_LOG(DEBUG) << "TokenAuthMethod::Authenticate 1"; // -AL- TEMP                  
    // add the token to the front of the headers. For consistency auth headers should be
    // at the front.
    const std::pair<std::string, std::string> token_header("authorization",
                                                           "Bearer " + token_);
    ARROW_LOG(DEBUG) << "TokenAuthMethod::Authenticate 2"; // -AL- TEMP  

    call_options.headers.insert(call_options.headers.begin(), token_header);

    ARROW_LOG(DEBUG) << "TokenAuthMethod::Authenticate 3"; // -AL- TEMP  

    // -AL- if issue got fixed, try reverting my change here.
        // const Status status = client_.Authenticate(
        // call_options, std::unique_ptr<ClientAuthHandler>(new NoOpClientAuthHandler()));
    std::unique_ptr<ClientAuthHandler> noop_handler = std::make_unique<NoOpClientAuthHandler>();
    // auto noop_handler = std::make_unique<NoOpClientAuthHandler>();
    ARROW_LOG(DEBUG) << "TokenAuthMethod::Authenticate 3.1"; // -AL- TEMP 
    const Status status = client_.Authenticate(
        call_options, std::move(noop_handler)); // -AL- this line triggers `NoOpClientAuthHandler`

    ARROW_LOG(DEBUG) << "TokenAuthMethod::Authenticate 4"; // -AL- TEMP  
    if (!status.ok()) {
      ARROW_LOG(DEBUG) << "TokenAuthMethod::Authenticate 5"; // -AL- TEMP  
      const auto& flight_status = FlightStatusDetail::UnwrapStatus(status);
      ARROW_LOG(DEBUG) << "TokenAuthMethod::Authenticate 6"; // -AL- TEMP  
      if (flight_status != nullptr) {
        ARROW_LOG(DEBUG) << "TokenAuthMethod::Authenticate 7"; // -AL- TEMP  
        if (flight_status->code() == FlightStatusCode::Unauthenticated) {
          ARROW_LOG(DEBUG) << "TokenAuthMethod::Authenticate 8"; // -AL- TEMP  
          throw AuthenticationException("Failed to authenticate with token: " + token_ +
                                        " Message: " + status.message());
        } else if (flight_status->code() == FlightStatusCode::Unavailable) {
          ARROW_LOG(DEBUG) << "TokenAuthMethod::Authenticate 9"; // -AL- TEMP  
          throw CommunicationException(status.message());
        }
      }
      ARROW_LOG(DEBUG) << "TokenAuthMethod::Authenticate 10"; // -AL- TEMP  
      throw DriverException(status.message());
    }
    ARROW_LOG(DEBUG) << "TokenAuthMethod::Authenticate 11"; // -AL- TEMP  
  }
};
}  // namespace

std::unique_ptr<FlightSqlAuthMethod> FlightSqlAuthMethod::FromProperties(
    const std::unique_ptr<FlightClient>& client,
    const Connection::ConnPropertyMap& properties) {
  // Check if should use user-password authentication
  auto it_user = properties.find(std::string(FlightSqlConnection::USER));
  if (it_user == properties.end()) {
    // The Microsoft OLE DB to ODBC bridge provider (MSDASQL) will write
    // "User ID" and "Password" properties instead of mapping
    // to ODBC compliant UID/PWD keys.
    it_user = properties.find(std::string(FlightSqlConnection::USER_ID));
  }

  auto it_password = properties.find(std::string(FlightSqlConnection::PASSWORD));
  auto it_token = properties.find(std::string(FlightSqlConnection::TOKEN));

  if (it_user == properties.end() || it_password == properties.end()) {
    // Accept UID/PWD as aliases for User/Password. These are suggested as
    // standard properties in the documentation for SQLDriverConnect.
    it_user = properties.find(std::string(FlightSqlConnection::UID));
    it_password = properties.find(std::string(FlightSqlConnection::PWD));
  }
  if (it_user != properties.end() || it_password != properties.end()) {
    const std::string& user = it_user != properties.end() ? it_user->second : "";
    const std::string& password =
        it_password != properties.end() ? it_password->second : "";

    return std::unique_ptr<FlightSqlAuthMethod>(
        new UserPasswordAuthMethod(*client, user, password));
  } else if (it_token != properties.end()) {
    const auto& token = it_token->second;
    return std::unique_ptr<FlightSqlAuthMethod>(new TokenAuthMethod(*client, token));
  }

  return std::unique_ptr<FlightSqlAuthMethod>(new NoOpAuthMethod);
}

}  // namespace arrow::flight::sql::odbc
