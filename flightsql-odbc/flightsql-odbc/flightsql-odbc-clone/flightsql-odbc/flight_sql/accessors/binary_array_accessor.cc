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

#include "binary_array_accessor.h"

#include <arrow/array.h>
#include <algorithm>
#include <cstdint>

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

namespace {

inline void MoveSingleCellToBinaryBuffer(ColumnBinding *binding,
                                         BinaryArray *array, int64_t i,
                                         int64_t &value_offset, bool update_value_offset, odbcabstraction::Diagnostics &diagnostics) {

  const char *value = array->Value(i).data();
  size_t size_in_bytes = array->value_length(i);

  size_t remaining_length = static_cast<size_t>(size_in_bytes - value_offset);
  size_t value_length =
      std::min(remaining_length,
               binding->buffer_length);

  auto *byte_buffer = static_cast<unsigned char *>(binding->buffer) +
                      i * binding->buffer_length;
  memcpy(byte_buffer, ((char *)value) + value_offset, value_length);

  if (remaining_length > binding->buffer_length) {
    diagnostics.AddTruncationWarning();
    if (update_value_offset) {
      value_offset += value_length;
    }
  } else if (update_value_offset) {
    value_offset = -1;
  }

  if (binding->strlen_buffer) {
    binding->strlen_buffer[i] = static_cast<ssize_t>(remaining_length);
  }
}

} // namespace

template <CDataType TARGET_TYPE>
BinaryArrayFlightSqlAccessor<TARGET_TYPE>::BinaryArrayFlightSqlAccessor(
    Array *array)
    : FlightSqlAccessor<BinaryArray, TARGET_TYPE,
                        BinaryArrayFlightSqlAccessor<TARGET_TYPE>>(array) {}

template <>
void BinaryArrayFlightSqlAccessor<CDataType_BINARY>::MoveSingleCell_impl(
    ColumnBinding *binding, BinaryArray *array, int64_t i,
    int64_t &value_offset, bool update_value_offset, odbcabstraction::Diagnostics &diagnostics) {
  MoveSingleCellToBinaryBuffer(binding, array, i, value_offset, update_value_offset, diagnostics);
}

template <CDataType TARGET_TYPE>
size_t BinaryArrayFlightSqlAccessor<TARGET_TYPE>::GetCellLength_impl(ColumnBinding *binding) const {
  return binding->buffer_length;
}

template class BinaryArrayFlightSqlAccessor<odbcabstraction::CDataType_BINARY>;

} // namespace flight_sql
} // namespace driver
