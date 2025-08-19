# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Use C++ 20 for ODBC and its subdirectory
# GH-44792: Arrow will switch to C++ 20
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_custom_target(arrow_flight_sql_odbc)

# Ensure fmt is loaded as header only
add_compile_definitions(FMT_HEADER_ONLY)

if(WIN32)
  if(MSVC_VERSION GREATER_EQUAL 1900)
    set(ODBCINST legacy_stdio_definitions odbccp32 shlwapi)
  elseif(MINGW)
    set(ODBCINST odbccp32 shlwapi)
  endif()
elseif(APPLE)
  set(ODBCINST iodbcinst)
else()
  set(ODBCINST odbcinst)
endif()

add_definitions(-DUNICODE=1)

include(FetchContent)
fetchcontent_declare(spdlog
                     URL https://github.com/gabime/spdlog/archive/refs/tags/v1.15.3.zip
                         CONFIGURE_COMMAND
                         ""
                         BUILD_COMMAND
                         "")
fetchcontent_makeavailable(spdlog)

add_subdirectory(flight_sql)
add_subdirectory(odbcabstraction)
add_subdirectory(tests)

arrow_install_all_headers("arrow/flight/sql/odbc")

set(ARROW_FLIGHT_SQL_ODBC_SRCS entry_points.cc odbc_api.cc)

if(WIN32)
  list(APPEND ARROW_FLIGHT_SQL_ODBC_SRCS odbc.def)
endif()

add_arrow_lib(arrow_flight_sql_odbc
              CMAKE_PACKAGE_NAME
              ArrowFlightSqlOdbc
              PKG_CONFIG_NAME
              arrow-flight-sql-odbc
              OUTPUTS
              ARROW_FLIGHT_SQL_ODBC_LIBRARIES
              SOURCES
              ${ARROW_FLIGHT_SQL_ODBC_SRCS}
              DEPENDENCIES
              arrow_flight_sql
              SHARED_LINK_FLAGS
              ${ARROW_VERSION_SCRIPT_FLAGS} # Defined in cpp/arrow/CMakeLists.txt
              SHARED_LINK_LIBS
              arrow_flight_sql_shared
              SHARED_INSTALL_INTERFACE_LIBS
              ArrowFlight::arrow_flight_sql_shared
              STATIC_LINK_LIBS
              arrow_flight_sql_static
              STATIC_INSTALL_INTERFACE_LIBS
              ArrowFlight::arrow_flight_sql_static
              SHARED_PRIVATE_LINK_LIBS
              ${ODBC_LIBRARIES}
              ${ODBCINST}
              odbcabstraction
              arrow_odbc_spi_impl
              spdlog::spdlog)

foreach(LIB_TARGET ${ARROW_FLIGHT_SQL_ODBC_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_FLIGHT_SQL_ODBC_EXPORTING)
endforeach()

# Construct ODBC Windows installer

if(ARROW_FLIGHT_SQL_ODBC_INSTALLER)
  # -AL- try to find the path for "release/Release"
  message(STATUS "-AL- CMAKE_INSTALL_LIBDIR = ${CMAKE_INSTALL_LIBDIR}")
  message(STATUS "-AL- CMAKE_CURRENT_LIST_DIR = ${CMAKE_CURRENT_LIST_DIR}")
  message(STATUS "-AL- CMAKE_INSTALL_INCLUDEDIR = ${CMAKE_INSTALL_INCLUDEDIR}")
  message(STATUS "-AL- CMAKE_INSTALL_BINDIR = ${CMAKE_INSTALL_BINDIR}")
  message(STATUS "-AL- ARROW_CMAKE_DIR = ${ARROW_CMAKE_DIR}")
  message(STATUS "-AL- ARROW_CMAKE_PACKAGE_NAME = ${ARROW_CMAKE_PACKAGE_NAME}")
  message(STATUS "-AL- ARROW_CMAKE_DIR/ARROW_CMAKE_PACKAGE_NAME = ${ARROW_CMAKE_DIR}/${ARROW_CMAKE_PACKAGE_NAME}"
  )
  message(STATUS "-AL- CMAKE_RUNTIME_OUTPUT_DIRECTORY = ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}"
  )
  message(STATUS "-AL- BUILD_OUTPUT_ROOT_DIRECTORY = ${BUILD_OUTPUT_ROOT_DIRECTORY}")
  message(STATUS "-AL- CMAKE_BUILD_TYPE = ${CMAKE_BUILD_TYPE}")
  message(STATUS "-AL- $BUILD_OUTPUT_ROOT_DIRECTORY$CMAKE_BUILD_TYPE = ${BUILD_OUTPUT_ROOT_DIRECTORY}${CMAKE_BUILD_TYPE}"
  )
  message(STATUS "-AL- CMAKE_INSTALL_PREFIX = ${CMAKE_INSTALL_PREFIX}")
  message(STATUS "-AL- CMAKE_SOURCE_DIR = ${CMAKE_SOURCE_DIR}")

  include(InstallRequiredSystemLibraries)

  set(CPACK_RESOURCE_FILE_LICENSE
      "${CMAKE_CURRENT_SOURCE_DIR}/../../../../../../LICENSE.txt")
  # Tentative version 1.0.0 (patch version is not set)
  set(CPACK_PACKAGE_VERSION_MAJOR "1")
  set(CPACK_PACKAGE_VERSION_MINOR "0")

  set(CPACK_PACKAGE_NAME "Apache Arrow Flight SQL ODBC")
  set(CPACK_PACKAGE_VENDOR "Apache Arrow")
  set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Apache Arrow Flight SQL ODBC Driver")
  set(CPACK_PACKAGE_CONTACT "#TODO arrow maintainers")

  install(DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}${CMAKE_BUILD_TYPE}/"
          DESTINATION bin
          COMPONENT flight_sql_odbc_lib
          FILES_MATCHING
          PATTERN "*.dll"
          PATTERN "arrow_flight_testing.dll" EXCLUDE
          PATTERN "arrow_testing.dll" EXCLUDE
          PATTERN "gflags.dll" EXCLUDE
          PATTERN "gmock.dll" EXCLUDE
          PATTERN "gtest.dll" EXCLUDE
          PATTERN "gtest_main.dll" EXCLUDE)

  get_cmake_property(CPACK_COMPONENTS_ALL COMPONENTS)
  # Returns: 1>-- -AL- CPACK_COMPONENTS_ALL before = Unspecified;arrow_compute_shared;arrow_flight_shared;arrow_flight_sql_odbc_shared;arrow_flight_sql_shared;arrow_flight_testing_shared;arrow_shared;arrow_testing_shared;library
  message(STATUS "-AL- CPACK_COMPONENTS_ALL before = ${CPACK_COMPONENTS_ALL}")
  # list(REMOVE_ITEM CPACK_COMPONENTS_ALL "arrow_compute_shared" "arrow_flight_shared" "arrow_flight_sql_odbc_shared" "arrow_flight_sql_shared" "arrow_flight_testing_shared" "arrow_shared" "arrow_testing_shared")
  set(CPACK_COMPONENTS_ALL Unspecified)
  list(APPEND CPACK_COMPONENTS_ALL "flight_sql_odbc_lib")

  message(STATUS "-AL- CPACK_COMPONENTS_ALL after = ${CPACK_COMPONENTS_ALL}")

  include(CPack)

  cpack_add_component(flight_sql_odbc_lib DISPLAY_NAME "ODBC library"
      DESCRIPTION
  "ODBC library bin, required to install"
      REQUIRED)

  message(STATUS "-AL- CPACK_COMPONENTS_ALL last = ${CPACK_COMPONENTS_ALL}")
endif()
