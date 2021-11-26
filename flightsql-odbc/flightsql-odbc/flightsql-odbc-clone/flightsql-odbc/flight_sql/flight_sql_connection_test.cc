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

#include "flight_sql_connection.h"
#include "gtest/gtest.h"
#include <arrow/flight/types.h>

namespace driver {
namespace flight_sql {

using arrow::flight::Location;
using arrow::flight::TimeoutDuration;
using spi::Connection;

TEST(AttributeTests, SetAndGetAttribute) {
  FlightSqlConnection connection(spi::V_3);

  connection.SetAttribute(Connection::CONNECTION_TIMEOUT, 200);
  const boost::optional<Connection::Attribute> firstValue =
      connection.GetAttribute(Connection::CONNECTION_TIMEOUT);

  EXPECT_TRUE(firstValue.has_value());

  EXPECT_EQ(boost::get<int>(firstValue.value()), 200);

  connection.SetAttribute(Connection::CONNECTION_TIMEOUT, 300);

  const boost::optional<Connection::Attribute> changeValue =
      connection.GetAttribute(Connection::CONNECTION_TIMEOUT);

  EXPECT_TRUE(changeValue.has_value());
  EXPECT_EQ(boost::get<int>(changeValue.value()), 300);

  connection.Close();
}

TEST(AttributeTests, GetAttributeWithoutSetting) {
  FlightSqlConnection connection(spi::V_3);

  const boost::optional<Connection::Attribute> anOptional =
      connection.GetAttribute(Connection::CONNECTION_TIMEOUT);

  EXPECT_FALSE(anOptional.has_value());

  connection.Close();
}

TEST(BuildLocationTests, ForTcp) {
  const Location &actual_location1 = FlightSqlConnection::BuildLocation({
      {Connection::HOST, std::string("localhost")},
      {Connection::PORT, 32010},
  });
  const Location &actual_location2 = FlightSqlConnection::BuildLocation({
      {Connection::HOST, std::string("localhost")},
      {Connection::PORT, 32011},
  });

  Location expected_location;
  ASSERT_TRUE(
      Location::ForGrpcTcp("localhost", 32010, &expected_location).ok());
  ASSERT_EQ(expected_location, actual_location1);
  ASSERT_NE(expected_location, actual_location2);
}

TEST(BuildLocationTests, ForTls) {
  const Location &actual_location1 = FlightSqlConnection::BuildLocation({
      {Connection::HOST, std::string("localhost")},
      {Connection::PORT, 32010},
      {Connection::USE_TLS, true},
  });
  const Location &actual_location2 = FlightSqlConnection::BuildLocation({
      {Connection::HOST, std::string("localhost")},
      {Connection::PORT, 32011},
      {Connection::USE_TLS, true},
  });

  Location expected_location;
  ASSERT_TRUE(
      Location::ForGrpcTls("localhost", 32010, &expected_location).ok());
  ASSERT_EQ(expected_location, actual_location1);
  ASSERT_NE(expected_location, actual_location2);
}

TEST(BuildCallOptionsTest, ConnectionTimeout) {
  FlightSqlConnection connection(spi::V_3);

  // Expect default timeout to be -1
  ASSERT_EQ(TimeoutDuration{-1.0}, connection.BuildCallOptions().timeout);

  connection.SetAttribute(Connection::CONNECTION_TIMEOUT, 10.0);
  ASSERT_EQ(TimeoutDuration{10.0}, connection.BuildCallOptions().timeout);
}

} // namespace flight_sql
} // namespace driver
