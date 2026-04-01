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

#include "arrow/flight/server_auth.h"

#include "arrow/util/logging.h" // -AL- TEMP

namespace arrow {
namespace flight {

ServerAuthHandler::~ServerAuthHandler() {}
  // -AL- todo: write class read/write auth handler for our mock server.
  // I think it is needed for stable behavior.
NoOpAuthHandler::~NoOpAuthHandler() {}
Status NoOpAuthHandler::Authenticate(const ServerCallContext& context,
                                     ServerAuthSender* outgoing,
                                     ServerAuthReader* incoming) {
    // -AL- If this approach does work, I need to create a new handler to for the mock server.
    ARROW_LOG(DEBUG) << "NoOpAuthHandler::Authenticate - Reading response from client";
    std::string client_token;
    RETURN_NOT_OK(incoming->Read(&client_token));  // Read client's message
     ARROW_LOG(DEBUG) << "NoOpAuthHandler::Authenticate - client_token: " << client_token;
    // -AL- 
ARROW_LOG(DEBUG) << "NoOpAuthHandler::Authenticate - write response from server";
    // Validate token, then write response
    RETURN_NOT_OK(outgoing->Write("server-response"));  // Write response!
  return Status::OK();
}

Status NoOpAuthHandler::IsValid(const ServerCallContext& context,
                                const std::string& token, std::string* peer_identity) {
  *peer_identity = "";
  return Status::OK();
}

}  // namespace flight
}  // namespace arrow
