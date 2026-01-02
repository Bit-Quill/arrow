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

#include <gtest/gtest.h>

namespace arrow::flight::sql::odbc {

// A global test "environment", to delay 1 second between tests

  // -AL- this test set up adds 1 second before ALL tests, need to change it to be for every test
class DelayTestEnvironment : public ::testing::Environment {
 public:
  void SetUp() override { std::this_thread::sleep_for(std::chrono::seconds(1)); }
};

::testing::Environment* delay_env =
    ::testing::AddGlobalTestEnvironment(new DelayTestEnvironment);

}  // namespace arrow::flight::sql::odbc
