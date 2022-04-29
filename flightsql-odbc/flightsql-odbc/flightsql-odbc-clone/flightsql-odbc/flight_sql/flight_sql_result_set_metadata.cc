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

#include "flight_sql_result_set_metadata.h"
#include <odbcabstraction/platform.h>
#include "arrow/flight/sql/column_metadata.h"
#include "arrow/util/key_value_metadata.h"
#include "utils.h"

#include <odbcabstraction/exceptions.h>
#include <utility>

namespace driver {
namespace flight_sql {

using namespace odbcabstraction;
using arrow::DataType;
using arrow::Field;
using arrow::util::make_optional;
using arrow::util::nullopt;

constexpr int32_t StringColumnLength = 1024; // TODO: Get from connection

size_t FlightSqlResultSetMetadata::GetColumnCount() {
  return schema_->num_fields();
}

std::string FlightSqlResultSetMetadata::GetColumnName(int column_position) {
  return schema_->field(column_position - 1)->name();
}

std::string FlightSqlResultSetMetadata::GetName(int column_position) {
  return schema_->field(column_position - 1)->name();
}

size_t FlightSqlResultSetMetadata::GetPrecision(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata(schema_->field(column_position - 1)->metadata());
  const auto &result = metadata.GetPrecision();
  ThrowIfNotOK(result.status());
  return result.ValueOrDie();
}

size_t FlightSqlResultSetMetadata::GetScale(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata(schema_->field(column_position - 1)->metadata());
  const auto &result = metadata.GetScale();
  ThrowIfNotOK(result.status());
  return result.ValueOrDie();
}

SqlDataType FlightSqlResultSetMetadata::GetDataType(int column_position) {
  const std::shared_ptr<Field> &field = schema_->field(column_position - 1);
  return GetDataTypeFromArrowField_V3(field);
}

driver::odbcabstraction::Nullability
FlightSqlResultSetMetadata::IsNullable(int column_position) {
  const std::shared_ptr<Field> &field = schema_->field(column_position - 1);
  return field->nullable() ? odbcabstraction::NULLABILITY_NULLABLE : odbcabstraction::NULLABILITY_NO_NULLS;
}

std::string FlightSqlResultSetMetadata::GetSchemaName(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata(schema_->field(column_position - 1)->metadata());

  return metadata.GetSchemaName().ValueOrElse([] { return ""; });
}

std::string FlightSqlResultSetMetadata::GetCatalogName(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata(schema_->field(column_position - 1)->metadata());

  return metadata.GetCatalogName().ValueOrElse([] { return ""; });
}

std::string FlightSqlResultSetMetadata::GetTableName(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata(schema_->field(column_position - 1)->metadata());

  return metadata.GetTableName().ValueOrElse([] { return ""; });
}

std::string FlightSqlResultSetMetadata::GetColumnLabel(int column_position) {
  return schema_->field(column_position - 1)->name();
}

size_t FlightSqlResultSetMetadata::GetColumnDisplaySize(
    int column_position) {
  const std::shared_ptr<Field> &field = schema_->field(column_position - 1);
  arrow::flight::sql::ColumnMetadata metadata(field->metadata());

  int32_t column_size = metadata.GetPrecision().ValueOrElse([] { return StringColumnLength; });
  SqlDataType data_type_v3 = GetDataTypeFromArrowField_V3(field);

  return GetDisplaySize(data_type_v3, column_size).value_or(NO_TOTAL);
}

std::string FlightSqlResultSetMetadata::GetBaseColumnName(int column_position) {
  return schema_->field(column_position - 1)->name();
}

std::string FlightSqlResultSetMetadata::GetBaseTableName(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata(schema_->field(column_position - 1)->metadata());
  return metadata.GetTableName().ValueOrElse([] { return ""; });
}

std::string FlightSqlResultSetMetadata::GetConciseType(int column_position) {
  // TODO Implement after the PR from column metadata is merged
  return "";
}

size_t FlightSqlResultSetMetadata::GetLength(int column_position) {
  const std::shared_ptr<Field> &field = schema_->field(column_position - 1);
  arrow::flight::sql::ColumnMetadata metadata(field->metadata());

  int32_t column_size = metadata.GetPrecision().ValueOrElse([] { return StringColumnLength; });
  SqlDataType data_type_v3 = GetDataTypeFromArrowField_V3(field);

  return GetBufferLength(data_type_v3, column_size).value_or(NO_TOTAL);
}

std::string FlightSqlResultSetMetadata::GetLiteralPrefix(int column_position) {
  // TODO: Flight SQL column metadata does not have this, should we add to the spec?
  return "";
}

std::string FlightSqlResultSetMetadata::GetLiteralSuffix(int column_position) {
  // TODO: Flight SQL column metadata does not have this, should we add to the spec?
  return "";
}

std::string FlightSqlResultSetMetadata::GetLocalTypeName(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata(schema_->field(column_position - 1)->metadata());

  // TODO: Is local type name the same as type name?
  return metadata.GetTypeName().ValueOrElse([] { return ""; });
}

size_t FlightSqlResultSetMetadata::GetNumPrecRadix(int column_position) {
  const std::shared_ptr<Field> &field = schema_->field(column_position - 1);
  arrow::flight::sql::ColumnMetadata metadata(field->metadata());

  SqlDataType data_type_v3 = GetDataTypeFromArrowField_V3(field);

  return GetRadixFromSqlDataType(data_type_v3).value_or(NO_TOTAL);
}

size_t FlightSqlResultSetMetadata::GetOctetLength(int column_position) {
  const std::shared_ptr<Field> &field = schema_->field(column_position - 1);
  arrow::flight::sql::ColumnMetadata metadata(field->metadata());

  int32_t column_size = metadata.GetPrecision().ValueOrElse([] { return StringColumnLength; });
  SqlDataType data_type_v3 = GetDataTypeFromArrowField_V3(field);

  return GetCharOctetLength(data_type_v3, column_size).value_or(NO_TOTAL);
}

std::string FlightSqlResultSetMetadata::GetTypeName(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata(schema_->field(column_position - 1)->metadata());

  return metadata.GetTypeName().ValueOrElse([] { return ""; });
}

driver::odbcabstraction::Updatability
FlightSqlResultSetMetadata::GetUpdatable(int column_position) {
  return odbcabstraction::UPDATABILITY_READWRITE_UNKNOWN;
}

bool FlightSqlResultSetMetadata::IsAutoUnique(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata(schema_->field(column_position - 1)->metadata());

  // TODO: Is AutoUnique equivalent to AutoIncrement?
  return metadata.GetIsAutoIncrement().ValueOrElse([] { return false; });
}

bool FlightSqlResultSetMetadata::IsCaseSensitive(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata(schema_->field(column_position - 1)->metadata());

  return metadata.GetIsCaseSensitive().ValueOrElse([] { return false; });
}

driver::odbcabstraction::Searchability
FlightSqlResultSetMetadata::IsSearchable(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata(schema_->field(column_position - 1)->metadata());

  bool is_searchable = metadata.GetIsSearchable().ValueOrElse([] { return false; });
  return is_searchable ? odbcabstraction::SEARCHABILITY_ALL : odbcabstraction::SEARCHABILITY_NONE;
}

bool FlightSqlResultSetMetadata::IsUnsigned(int column_position) {
  const std::shared_ptr<Field> &field = schema_->field(column_position - 1);

  switch (field->type()->id()) {
    case arrow::Type::UINT8:
    case arrow::Type::UINT16:
    case arrow::Type::UINT32:
    case arrow::Type::UINT64:
      return true;
    default:
      return false;
  }
}

bool FlightSqlResultSetMetadata::IsFixedPrecScale(int column_position) {
  // TODO: Flight SQL column metadata does not have this, should we add to the spec?
  return false;
}

FlightSqlResultSetMetadata::FlightSqlResultSetMetadata(
    std::shared_ptr<arrow::Schema> schema)
    : schema_(std::move(schema)) {}

FlightSqlResultSetMetadata::FlightSqlResultSetMetadata(
    const std::shared_ptr<arrow::flight::FlightInfo> &flight_info) {
  arrow::ipc::DictionaryMemo dict_memo;

  ThrowIfNotOK(flight_info->GetSchema(&dict_memo, &schema_));
}

} // namespace flight_sql
} // namespace driver