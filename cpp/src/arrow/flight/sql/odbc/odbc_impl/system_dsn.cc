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

#include "arrow/flight/sql/odbc/odbc_impl/system_dsn.h"

#include "arrow/result.h"
#include "arrow/util/utf8.h"

#include <boost/algorithm/string/predicate.hpp>
#include <sstream>

#ifdef __linux__
#  define GET_SQWCHAR_PTR(wstring_var) (ToSqlWCharVector(wstring_var).data())
#else
// Windows and macOS
#  define GET_SQWCHAR_PTR(wstring_var) (wstring_var.c_str())
#endif

namespace arrow::flight::sql::odbc {

using config::Configuration;

void PostError(DWORD error_code, LPWSTR error_msg) {
#if defined _WIN32
  MessageBox(NULL, error_msg, L"Error!", MB_ICONEXCLAMATION | MB_OK);
#endif  // _WIN32
  SQLPostInstallerError(error_code, error_msg);
}

void PostArrowUtilError(arrow::Status error_status) {
  std::string error_msg = error_status.message();
  std::wstring werror_msg = arrow::util::UTF8ToWideString(error_msg).ValueOr(
      L"Error during utf8 to wide string conversion");

  PostError(ODBC_ERROR_GENERAL_ERR, const_cast<LPWSTR>(werror_msg.c_str()));
}

void PostLastInstallerError() {
#define BUFFER_SIZE (1024)
  DWORD code;
  std::vector<SQLWCHAR> msg(BUFFER_SIZE);
  SQLInstallerError(1, &code, msg.data(), BUFFER_SIZE, NULL);

  std::wstringstream buf;
#ifdef __linux__
  buf << L"Message: \"";
  for (SQLWCHAR wch : msg) {
    buf << static_cast<wchar_t>(wch);
  }
  buf << L"\", Code: " << code;
#else
  // Windows and macOS
  buf << L"Message: \"" << msg.data() << L"\", Code: " << code;
#endif  // __linux__
  std::wstring error_msg = buf.str();

  PostError(code, const_cast<LPWSTR>(error_msg.c_str()));
}

std::vector<SQLWCHAR> ToSqlWCharVector(const std::wstring& ws) {
  std::vector<SQLWCHAR> buf;
  buf.assign(ws.begin(), ws.end());
  return buf;
}

/**
 * Unregister specified DSN.
 *
 * @param dsn DSN name.
 * @return True on success and false on fail.
 */
bool UnregisterDsn(const std::wstring& dsn) {
  if (SQLRemoveDSNFromIni(GET_SQWCHAR_PTR(dsn))) {
    return true;
  }

  PostLastInstallerError();
  return false;
}

/**
 * Register DSN with specified configuration.
 *
 * @param config Configuration.
 * @param driver Driver.
 * @return True on success and false on fail.
 */
bool RegisterDsn(const Configuration& config, LPCWSTR driver) {
  const std::string& dsn = config.Get(FlightSqlConnection::DSN);
  auto wdsn_result = arrow::util::UTF8ToWideString(dsn);
  if (!wdsn_result.status().ok()) {
    PostArrowUtilError(wdsn_result.status());
    return false;
  }
  std::wstring wdsn = wdsn_result.ValueOrDie();

  if (!SQLWriteDSNToIni(GET_SQWCHAR_PTR(wdsn), driver)) {
    PostLastInstallerError();
    return false;
  }

  const auto& map = config.GetProperties();
  for (auto it = map.begin(); it != map.end(); ++it) {
    std::string_view key = it->first;
    if (boost::iequals(FlightSqlConnection::DSN, key) ||
        boost::iequals(FlightSqlConnection::DRIVER, key)) {
      continue;
    }

    auto wkey_result = arrow::util::UTF8ToWideString(key);
    if (!wkey_result.status().ok()) {
      PostArrowUtilError(wkey_result.status());
      return false;
    }
    std::wstring wkey = wkey_result.ValueOrDie();

    auto wvalue_result = arrow::util::UTF8ToWideString(it->second);
    if (!wvalue_result.status().ok()) {
      PostArrowUtilError(wvalue_result.status());
      return false;
    }
    std::wstring wvalue = wvalue_result.ValueOrDie();

    if (!SQLWritePrivateProfileString(GET_SQWCHAR_PTR(wdsn), GET_SQWCHAR_PTR(wkey),
                                      GET_SQWCHAR_PTR(wvalue),
                                      reinterpret_cast<LPCWSTR>(L"ODBC.INI"))) {
      PostLastInstallerError();
      return false;
    }
  }

  return true;
}
}  // namespace arrow::flight::sql::odbc
