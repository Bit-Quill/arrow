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

#include "arrow/flight/sql/client.h"
#include <odbcabstraction/statement.h>

#include <arrow/flight/api.h>
#include <arrow/flight/sql/api.h>
#include <arrow/flight/types.h>

namespace driver {
namespace flight_sql {

class FlightSqlStatement : public odbcabstraction::Statement {

private:
  std::map<StatementAttributeId, Attribute> attribute_;
  arrow::flight::FlightCallOptions call_options_;
  arrow::flight::sql::FlightSqlClient &sql_client_;
  std::shared_ptr<odbcabstraction::ResultSet> current_result_set_;
  std::shared_ptr<odbcabstraction::ResultSetMetadata>
      current_result_set_metadata_;
  std::shared_ptr<arrow::flight::sql::PreparedStatement> prepared_statement_;

public:
  FlightSqlStatement(arrow::flight::sql::FlightSqlClient &sql_client,
                     arrow::flight::FlightCallOptions call_options);

  void SetAttribute(StatementAttributeId attribute, const Attribute &value);

  boost::optional<Attribute> GetAttribute(StatementAttributeId attribute);

  boost::optional<std::shared_ptr<odbcabstraction::ResultSetMetadata>>
  Prepare(const std::string &query);

  bool ExecutePrepared();

  bool Execute(const std::string &query);

  std::shared_ptr<odbcabstraction::ResultSet> GetResultSet();

  long GetUpdateCount();

  std::shared_ptr<odbcabstraction::ResultSet>
  GetTables_V2(const std::string *catalog_name, const std::string *schema_name,
               const std::string *table_name, const std::string *table_type);

  std::shared_ptr<odbcabstraction::ResultSet>
  GetTables_V3(const std::string *catalog_name, const std::string *schema_name,
               const std::string *table_name, const std::string *table_type);

  std::shared_ptr<odbcabstraction::ResultSet>
  GetColumns_V2(const std::string *catalog_name, const std::string *schema_name,
                const std::string *table_name, const std::string *column_name);

  std::shared_ptr<odbcabstraction::ResultSet>
  GetColumns_V3(const std::string *catalog_name, const std::string *schema_name,
                const std::string *table_name, const std::string *column_name);

  std::shared_ptr<odbcabstraction::ResultSet> GetTypeInfo(int dataType);
};
} // namespace flight_sql
} // namespace driver
