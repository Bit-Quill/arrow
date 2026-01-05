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

#include <memory>

#include "arrow/flight/sql/odbc/odbc_impl/diagnostics.h"
#include "arrow/flight/sql/odbc/odbc_impl/types.h"

// -AL- below fix doesn't work for me either, maybe `mutex.h` is not included inside here yet.
// HACK: Workaround absl::Mutex ABI incompatibility by making sure the
// non-debug version of Abseil is included. Disables deadlock detection.
// (https://github.com/conda-forge/abseil-cpp-feedstock/issues/104,
//  https://github.com/abseil/abseil-cpp/issues/1624)

#  ifndef NDEBUG
#    define ARROW_NO_NDEBUG
#    define NDEBUG
#  endif

#  include <absl/synchronization/mutex.h>

#  ifdef ARROW_NO_NDEBUG
#    undef NDEBUG
#  endif

namespace arrow::flight::sql::odbc {

class Connection;

/// \brief High-level representation of an ODBC driver.
class Driver {
 protected:
  Driver() = default;

 public:
  virtual ~Driver() = default;

  /// \brief Create a connection using given ODBC version.
  /// \param odbc_version ODBC version to be used.
  virtual std::shared_ptr<Connection> CreateConnection(OdbcVersion odbc_version) = 0;

  /// \brief Gets the diagnostics for this connection.
  /// \return the diagnostics
  virtual Diagnostics& GetDiagnostics() = 0;

  /// \brief Sets the driver version.
  virtual void SetVersion(std::string version) = 0;

  /// \brief Register a log to be used by the system.
  virtual void RegisterLog() = 0;
};

}  // namespace arrow::flight::sql::odbc
