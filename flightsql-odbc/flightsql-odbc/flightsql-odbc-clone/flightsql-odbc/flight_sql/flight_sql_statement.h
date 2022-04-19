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
#include "flight_sql_statement_get_tables.h"
#include <odbcabstraction/statement.h>
#include <odbcabstraction/diagnostics.h>

#include <arrow/flight/api.h>
#include <arrow/flight/sql/api.h>
#include <arrow/flight/types.h>

namespace driver {
namespace flight_sql {

class FlightSqlStatement : public odbcabstraction::Statement {

private:
  odbcabstraction::Diagnostics diagnostics_;
  std::map<StatementAttributeId, Attribute> attribute_;
  arrow::flight::FlightCallOptions call_options_;
  arrow::flight::sql::FlightSqlClient &sql_client_;
  std::shared_ptr<odbcabstraction::ResultSet> current_result_set_;
  std::shared_ptr<arrow::flight::sql::PreparedStatement> prepared_statement_;

  std::shared_ptr<odbcabstraction::ResultSet>
  GetTables(const std::string *catalog_name, const std::string *schema_name,
            const std::string *table_name, const std::string *table_type,
            const ColumnNames &column_names);

public:
  FlightSqlStatement(
      const odbcabstraction::Diagnostics &diagnostics,
      arrow::flight::sql::FlightSqlClient &sql_client,
      arrow::flight::FlightCallOptions call_options);

  bool SetAttribute(StatementAttributeId attribute, const Attribute &value) override;

  boost::optional<Attribute> GetAttribute(StatementAttributeId attribute) override;

  boost::optional<std::shared_ptr<odbcabstraction::ResultSetMetadata>>
  Prepare(const std::string &query) override;

  bool ExecutePrepared() override;

  bool Execute(const std::string &query) override;

  std::shared_ptr<odbcabstraction::ResultSet> GetResultSet() override;

  long GetUpdateCount() override;

  std::shared_ptr<odbcabstraction::ResultSet>
  GetTables_V2(const std::string *catalog_name, const std::string *schema_name,
               const std::string *table_name, const std::string *table_type) override;

  std::shared_ptr<odbcabstraction::ResultSet>
  GetTables_V3(const std::string *catalog_name, const std::string *schema_name,
               const std::string *table_name, const std::string *table_type) override;

  std::shared_ptr<odbcabstraction::ResultSet>
  GetColumns_V2(const std::string *catalog_name, const std::string *schema_name,
                const std::string *table_name, const std::string *column_name) override;

  std::shared_ptr<odbcabstraction::ResultSet>
  GetColumns_V3(const std::string *catalog_name, const std::string *schema_name,
                const std::string *table_name, const std::string *column_name) override;

  std::shared_ptr<odbcabstraction::ResultSet> GetTypeInfo(int dataType) override;

  odbcabstraction::Diagnostics &GetDiagnostics() override;
};
} // namespace flight_sql
} // namespace driver
