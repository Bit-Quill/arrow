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

#include "record_batch_transformer.h"
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/status.h>
#include <arrow/util/optional.h>

namespace driver {
namespace flight_sql {

using namespace arrow;
using arrow::util::optional;

class GetTablesReader {
private:
  std::shared_ptr<RecordBatch> record_batch_;
  int64_t current_row_;

public:
  explicit GetTablesReader(std::shared_ptr<RecordBatch> record_batch);

  bool Next();

  optional<std::string> GetCatalogName();

  optional<std::string> GetDbSchemaName();

  std::string GetTableName();

  std::string GetTableType();

  std::shared_ptr<Schema> GetSchema();
};

} // namespace flight_sql
} // namespace driver
