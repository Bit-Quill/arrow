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

#include <cassert>
#include <codecvt>
#include <cstring>
#include <locale>
#include <vector>
#include "arrow/flight/sql/odbc/odbc_impl/exceptions.h"
#include "arrow/util/macros.h"

#include <iostream>

#if defined(__APPLE__)
#  include <atomic>
#endif

namespace arrow::flight::sql::odbc {

#if defined(__APPLE__)
extern std::atomic<size_t> SqlWCharSize;

void ComputeSqlWCharSize();

inline size_t GetSqlWCharSize() {
  if (SqlWCharSize == 0) {
    ComputeSqlWCharSize();
  }

  return SqlWCharSize;
}
#else
constexpr inline size_t GetSqlWCharSize() { return sizeof(char16_t); }
#endif

template <typename CHAR_TYPE>
inline size_t wcsstrlen(const void* wcs_string) {
  size_t len;
  for (len = 0; ((CHAR_TYPE*)wcs_string)[len]; len++) {
  }
  return len;
}

inline size_t wcsstrlen(const void* wcs_string) {
  switch (GetSqlWCharSize()) {
    case sizeof(char16_t):
      return wcsstrlen<char16_t>(wcs_string);
    case sizeof(char32_t):
      return wcsstrlen<char32_t>(wcs_string);
    default:
      assert(false);
      throw DriverException("Encoding is unsupported, SQLWCHAR size: " +
                            std::to_string(GetSqlWCharSize()));
  }
}

// GH-46576: suppress unicode warnings
ARROW_SUPPRESS_DEPRECATION_WARNING
template <typename CHAR_TYPE>
inline void Utf8ToWcs(const char* utf8_string, size_t length,
                      std::vector<uint8_t>* result) {
  thread_local std::wstring_convert<std::codecvt_utf8<CHAR_TYPE>, CHAR_TYPE> converter;
  auto string = converter.from_bytes(utf8_string, utf8_string + length);

  uint32_t length_in_bytes = static_cast<uint32_t>(string.size() * GetSqlWCharSize());
  const uint8_t* data = (uint8_t*)string.data();

  result->reserve(length_in_bytes);
  result->assign(data, data + length_in_bytes);
}
ARROW_UNSUPPRESS_DEPRECATION_WARNING

inline void Utf8ToWcs(const char* utf8_string, size_t length,
                      std::vector<uint8_t>* result) {
  switch (GetSqlWCharSize()) {
    case sizeof(char16_t):
      return Utf8ToWcs<char16_t>(utf8_string, length, result);
    case sizeof(char32_t):
      return Utf8ToWcs<char32_t>(utf8_string, length, result);
    default:
      assert(false);
      throw DriverException("Encoding is unsupported, SQLWCHAR size: " +
                            std::to_string(GetSqlWCharSize()));
  }
}

inline void Utf8ToWcs(const char* utf8_string, std::vector<uint8_t>* result) {
  return Utf8ToWcs(utf8_string, strlen(utf8_string), result);
}

// GH-46576: suppress unicode warnings
ARROW_SUPPRESS_DEPRECATION_WARNING
template <typename CHAR_TYPE>
inline void WcsToUtf8(const void* wcs_string, size_t length_in_code_units,
                      std::vector<uint8_t>* result) {
  std::cout << "TestLog: Inside WcsToUtf8<CHAR_TYPE>() with wcs_string=" << (char*)wcs_string << ", length_in_code_units=" << length_in_code_units << std::endl;

  thread_local std::wstring_convert<std::codecvt_utf8<CHAR_TYPE>, CHAR_TYPE> converter;
  try {
    auto byte_string = converter.to_bytes((CHAR_TYPE*)wcs_string,
                                        (CHAR_TYPE*)wcs_string + length_in_code_units);

    uint32_t length_in_bytes = static_cast<uint32_t>(byte_string.size());
    const uint8_t* data = (uint8_t*)byte_string.data();

    result->reserve(length_in_bytes);
    result->assign(data, data + length_in_bytes);
  } catch (const std::exception& e) {
    std::cout << "TestLog: Caught exception during WcsToUtf8(): " << e.what()
              << std::endl;
  } catch (...) {
    std::cout << "Caught an unknown/non-standard exception during WcsToUtf8()" << std::endl;
  }
  std::cout << "TestLog: Exiting WcsToUtf8<CHAR_TYPE>()" << std::endl;
}
ARROW_UNSUPPRESS_DEPRECATION_WARNING

inline void WcsToUtf8(const void* wcs_string, size_t length_in_code_units,
                      std::vector<uint8_t>* result) {
  std::cout << "TestLog: Inside arrow::flight::sql::odbc::WcsToUtf8()" << std::endl;
  switch (GetSqlWCharSize()) {
    case sizeof(char16_t):
      std::cout << "TestLog: Calling WcsToUtf8<char16_t>()" << std::endl;
      return WcsToUtf8<char16_t>(wcs_string, length_in_code_units, result);
    case sizeof(char32_t):
      std::cout << "TestLog: Calling WcsToUtf8<char32_t>()" << std::endl;
      return WcsToUtf8<char32_t>(wcs_string, length_in_code_units, result);
    default:
      // assert(false);
      std::cout << "TestLog: Throwing DriverException" << std::endl;
      auto sqlwcharsize = GetSqlWCharSize();
      std::cout << "TestLog: sqlwcharsize = " << sqlwcharsize << std::endl;
      throw DriverException("Encoding is unsupported, SQLWCHAR size: " +
                            std::to_string(sqlwcharsize));
  }
}

inline void WcsToUtf8(const void* wcs_string, std::vector<uint8_t>* result) {
  std::cout << "TestLog: Inside arrow::flight::sql::odbc::WcsToUtf8()" << std::endl;
  auto wcs_len = wcsstrlen(wcs_string);
  std::cout << "TestLog: wcs_len = " << wcs_len << std::endl;
  return WcsToUtf8(wcs_string, wcs_len, result);
}

}  // namespace arrow::flight::sql::odbc
