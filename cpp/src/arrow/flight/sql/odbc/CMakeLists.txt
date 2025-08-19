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

# -AL- try to find the path for "release/Release"
message(STATUS "-AL- CMAKE_INSTALL_LIBDIR = ${CMAKE_INSTALL_LIBDIR}")
message(STATUS "-AL- CMAKE_CURRENT_LIST_DIR = ${CMAKE_CURRENT_LIST_DIR}")
message(STATUS "-AL- CMAKE_INSTALL_INCLUDEDIR = ${CMAKE_INSTALL_INCLUDEDIR}")
message(STATUS "-AL- CMAKE_INSTALL_BINDIR = ${CMAKE_INSTALL_BINDIR}")
message(STATUS "-AL- ARROW_CMAKE_DIR = ${ARROW_CMAKE_DIR}")
message(STATUS "-AL- ARROW_CMAKE_PACKAGE_NAME = ${ARROW_CMAKE_PACKAGE_NAME}")
message(STATUS "-AL- ARROW_CMAKE_DIR/ARROW_CMAKE_PACKAGE_NAME = ${ARROW_CMAKE_DIR}/${ARROW_CMAKE_PACKAGE_NAME}")
message(STATUS "-AL- CMAKE_RUNTIME_OUTPUT_DIRECTORY = ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}")
message(STATUS "-AL- BUILD_OUTPUT_ROOT_DIRECTORY = ${BUILD_OUTPUT_ROOT_DIRECTORY}")
message(STATUS "-AL- CMAKE_BUILD_TYPE = ${CMAKE_BUILD_TYPE}")
message(STATUS "-AL- $BUILD_OUTPUT_ROOT_DIRECTORY$CMAKE_BUILD_TYPE = ${BUILD_OUTPUT_ROOT_DIRECTORY}${CMAKE_BUILD_TYPE}")
message(STATUS "-AL- CMAKE_INSTALL_PREFIX = ${CMAKE_INSTALL_PREFIX}")
message(STATUS "-AL- CMAKE_SOURCE_DIR = ${CMAKE_SOURCE_DIR}")

# Update nsi variables
set(ODBC_DLL_DIR "${BUILD_OUTPUT_ROOT_DIRECTORY}${CMAKE_BUILD_TYPE}")

configure_file(
  "${CMAKE_CURRENT_LIST_DIR}/installer.nsi.in"
  "${CMAKE_CURRENT_LIST_DIR}/installer.nsi"
  @ONLY
)

# Create folder of dll files -AL-
set(FILTERED_INSTALL_DIR "${BUILD_OUTPUT_ROOT_DIRECTORY}_install_odbc_filtered") # Only DLLs copied here
file(GLOB ODBC_DLL_FILES "${BUILD_OUTPUT_ROOT_DIRECTORY}${CMAKE_BUILD_TYPE}/*.dll")
file(COPY ${ODBC_DLL_FILES} DESTINATION ${FILTERED_INSTALL_DIR})

add_custom_target(build_odbc_nsis_installer
    COMMAND makensis "${CMAKE_CURRENT_LIST_DIR}/installer.nsi"
)
