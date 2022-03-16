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

#include "flight_sql_statement.h"
#include "flight_sql_result_set.h"
#include "flight_sql_result_set_metadata.h"
#include "flight_sql_statement_get_columns.h"
#include "flight_sql_statement_get_tables.h"
#include "record_batch_transformer.h"
#include "utils.h"
#include <arrow/flight/sql/server.h>
#include <arrow/io/memory.h>

#include <boost/optional.hpp>
#include <odbcabstraction/exceptions.h>

namespace driver {
namespace flight_sql {

using arrow::Result;
using arrow::Status;
using arrow::flight::FlightCallOptions;
using arrow::flight::FlightClientOptions;
using arrow::flight::FlightInfo;
using arrow::flight::Location;
using arrow::flight::TimeoutDuration;
using arrow::flight::sql::FlightSqlClient;
using arrow::flight::sql::PreparedStatement;
using arrow::flight::sql::SqlSchema;
using driver::odbcabstraction::DriverException;
using driver::odbcabstraction::ResultSet;
using driver::odbcabstraction::ResultSetMetadata;
using driver::odbcabstraction::Statement;

namespace {

void ClosePreparedStatementIfAny(
    std::shared_ptr<arrow::flight::sql::PreparedStatement>
        &prepared_statement) {
  if (prepared_statement != nullptr) {
    ThrowIfNotOK(prepared_statement->Close());
    prepared_statement.reset();
  }
}

} // namespace

FlightSqlStatement::FlightSqlStatement(FlightSqlClient &sql_client,
                                       FlightCallOptions call_options)
    : sql_client_(sql_client), call_options_(call_options) {}

void FlightSqlStatement::SetAttribute(StatementAttributeId attribute,
                                      const Attribute &value) {
  attribute_[attribute] = value;
}

boost::optional<Statement::Attribute>
FlightSqlStatement::GetAttribute(StatementAttributeId attribute) {
  const auto &it = attribute_.find(attribute);
  return boost::make_optional(it != attribute_.end(), it->second);
}

boost::optional<std::shared_ptr<ResultSetMetadata>>
FlightSqlStatement::Prepare(const std::string &query) {
  ClosePreparedStatementIfAny(prepared_statement_);

  Result<std::shared_ptr<PreparedStatement>> result =
      sql_client_.Prepare(call_options_, query);
  ThrowIfNotOK(result.status());

  prepared_statement_ = *result;

  const auto &result_set_metadata =
      std::make_shared<FlightSqlResultSetMetadata>(
          prepared_statement_->dataset_schema());
  return boost::optional<std::shared_ptr<ResultSetMetadata>>(
      result_set_metadata);
}

bool FlightSqlStatement::ExecutePrepared() {
  assert(prepared_statement_.get() != nullptr);

  Result<std::shared_ptr<FlightInfo>> result = prepared_statement_->Execute();
  ThrowIfNotOK(result.status());

  current_result_set_ = std::make_shared<FlightSqlResultSet>(
      sql_client_, call_options_, result.ValueOrDie(), nullptr);

  return true;
}

bool FlightSqlStatement::Execute(const std::string &query) {
  ClosePreparedStatementIfAny(prepared_statement_);

  Result<std::shared_ptr<FlightInfo>> result =
      sql_client_.Execute(call_options_, query);
  ThrowIfNotOK(result.status());

  current_result_set_ = std::make_shared<FlightSqlResultSet>(
      sql_client_, call_options_, result.ValueOrDie(), nullptr);

  return true;
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetResultSet() {
  return current_result_set_;
}

long FlightSqlStatement::GetUpdateCount() { return -1; }

std::shared_ptr<odbcabstraction::ResultSet> FlightSqlStatement::GetTables(
    const std::string *catalog_name, const std::string *schema_name,
    const std::string *table_name, const std::string *table_type,
    const ColumnNames &column_names) {
  ClosePreparedStatementIfAny(prepared_statement_);

  std::vector<std::string> table_types;

  if ((catalog_name && *catalog_name == "%") &&
      (schema_name && schema_name->empty()) &&
      (table_name && table_name->empty()) &&
      (table_type && table_type->empty())) {
    current_result_set_ =
        GetTablesForSQLAllCatalogs(column_names, call_options_, sql_client_);
  } else if ((catalog_name && catalog_name->empty()) &&
             (schema_name && *schema_name == "%") &&
             (table_name && table_name->empty()) &&
             (table_type && table_type->empty())) {
    current_result_set_ = GetTablesForSQLAllDbSchemas(
        column_names, call_options_, sql_client_, schema_name);
  } else if ((catalog_name && catalog_name->empty()) &&
             (schema_name && schema_name->empty()) &&
             (table_name && table_name->empty()) &&
             (table_type && *table_type == "%")) {
    current_result_set_ =
        GetTablesForSQLAllTableTypes(column_names, call_options_, sql_client_);
  } else {
    if (table_type) {
      ParseTableTypes(*table_type, table_types);
    }

    current_result_set_ = GetTablesForGenericUse(
        column_names, call_options_, sql_client_, catalog_name, schema_name,
        table_name, table_types);
  }

  return current_result_set_;
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetTables_V2(
    const std::string *catalog_name, const std::string *schema_name,
    const std::string *table_name, const std::string *table_type) {
  ColumnNames column_names{"TABLE_QUALIFIER", "TABLE_OWNER", "TABLE_NAME",
                           "TABLE_TYPE", "REMARKS"};

  return GetTables(catalog_name, schema_name, table_name, table_type,
                   column_names);
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetTables_V3(
    const std::string *catalog_name, const std::string *schema_name,
    const std::string *table_name, const std::string *table_type) {
  ColumnNames column_names{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
                           "TABLE_TYPE", "REMARKS"};

  return GetTables(catalog_name, schema_name, table_name, table_type,
                   column_names);
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetColumns_V2(
    const std::string *catalog_name, const std::string *schema_name,
    const std::string *table_name, const std::string *column_name) {
  ClosePreparedStatementIfAny(prepared_statement_);

  Result<std::shared_ptr<FlightInfo>> result = sql_client_.GetTables(
      call_options_, catalog_name, schema_name, table_name, true, nullptr);
  ThrowIfNotOK(result.status());

  auto flight_info = result.ValueOrDie();

  auto transformer = std::make_shared<GetColumns_Transformer>(
      odbcabstraction::V_2, column_name);

  current_result_set_ = std::make_shared<FlightSqlResultSet>(
      sql_client_, call_options_, flight_info, transformer);

  return current_result_set_;
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetColumns_V3(
    const std::string *catalog_name, const std::string *schema_name,
    const std::string *table_name, const std::string *column_name) {
  ClosePreparedStatementIfAny(prepared_statement_);

  Result<std::shared_ptr<FlightInfo>> result = sql_client_.GetTables(
      call_options_, catalog_name, schema_name, table_name, true, nullptr);
  ThrowIfNotOK(result.status());

  auto flight_info = result.ValueOrDie();

  auto transformer = std::make_shared<GetColumns_Transformer>(
      odbcabstraction::V_3, column_name);

  current_result_set_ = std::make_shared<FlightSqlResultSet>(
      sql_client_, call_options_, flight_info, transformer);

  return current_result_set_;
}

std::shared_ptr<ResultSet> FlightSqlStatement::GetTypeInfo(int dataType) {
  return current_result_set_;
}

} // namespace flight_sql
} // namespace driver