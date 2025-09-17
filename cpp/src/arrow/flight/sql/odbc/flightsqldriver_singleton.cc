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

#include <memory>

#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/flight_sql_driver.h"

// Separate out FlightSqlDriver instance -AL- TODO need a comment here

std::shared_ptr<driver::flight_sql::FlightSqlDriver> getFlightSQLDriverInstance() {
  static auto instance = std::make_shared<driver::flight_sql::FlightSqlDriver>();
    return instance;
}
