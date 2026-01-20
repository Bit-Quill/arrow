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

# add_custom_target(tests) # -AL- I don't think this does anything.

if(WIN32)
  include_directories(${ODBC_INCLUDE_DIRS})
else()
  include_directories(${ODBC_INCLUDE_DIR})
endif()

find_package(SQLite3Alt REQUIRED)

set(ARROW_FLIGHT_SQL_MOCK_SERVER_SRCS
    ../../example/sqlite_sql_info.cc
    ../../example/sqlite_type_info.cc
    ../../example/sqlite_statement.cc
    ../../example/sqlite_statement_batch_reader.cc
    ../../example/sqlite_server.cc
    ../../example/sqlite_tables_schema_batch_reader.cc)

if(ARROW_TEST_LINKAGE STREQUAL "static")
  set(ARROW_FLIGHT_SQL_ODBC_TEST_LINK_LIBS arrow_odbc_spi_impl_static 
  # protobuf::libprotobuf # -AL- need to resolve double registration issue
  # ${ARROW_TEST_STATIC_LINK_LIBS}
  )
else()
  set(ARROW_FLIGHT_SQL_ODBC_TEST_LINK_LIBS arrow_odbc_spi_impl_shared 
  # protobuf::libprotobuf 
  # ${ARROW_TEST_SHARED_LINK_LIBS}
  )
endif()

if(WIN32)
add_arrow_test(flight_sql_odbc_test
               SOURCES
               odbc_test_suite.cc
               odbc_test_suite.h
               columns_test.cc
               connection_attr_test.cc
               connection_info_test.cc
               connection_test.cc
               errors_test.cc
               get_functions_test.cc
               statement_attr_test.cc
               statement_test.cc
               tables_test.cc
               type_info_test.cc
               # Enable Protobuf cleanup after test execution
               # GH-46889: move protobuf_test_util to a more common location
               ../../../../engine/substrait/protobuf_test_util.cc
               ${ARROW_FLIGHT_SQL_MOCK_SERVER_SRCS}
               EXTRA_LINK_LIBS
               ${ODBC_LIBRARIES}
               ${ODBCINST}
               ${SQLite3_LIBRARIES}
               arrow_odbc_spi_impl_shared)
else()
  # unix

  # -AL- todo after ODBC can build statically, resolve 
  # ODBC test issue. It doesn't need to change, but may need to act differently depending
  # on `arrow_odbc_spi_impl` change.
              #  # -AL- 1st difference from Windows build
              #  DEFINITIONS
              #  UNICODE

# -AL- try converting to add_executable to get rid of protobuf
# add_arrow_test(flight_sql_odbc_test
#                SOURCES
#                odbc_test_suite.cc
#                odbc_test_suite.h
#                columns_test.cc
#                connection_attr_test.cc
#                connection_info_test.cc
#                connection_test.cc
#                errors_test.cc
#                get_functions_test.cc
#                statement_attr_test.cc
#                statement_test.cc
#                tables_test.cc
#                type_info_test.cc
#                # Enable Protobuf cleanup after test execution
#                # GH-46889: move protobuf_test_util to a more common location
#                # ../../../../engine/substrait/protobuf_test_util.cc
#                # -AL- todo raise GitHub issue to enable this on macOS/Linux where static ODBC is needed.
#                ${ARROW_FLIGHT_SQL_MOCK_SERVER_SRCS}
#                # -AL- 1st difference from Windows build
#                DEFINITIONS
#                UNICODE
#                EXTRA_LINK_LIBS
#                # -AL- below commented out code are tries to get the protobuf issue resolved.
#               #  arrow_flight_testing_shared # -AL- tentative, doesn't fix the issue.
#               #  arrow_flight_sql_shared # -AL- tentative, doesn't fix the issue.
#                # ${ARROW_TEST_LINK_LIBS} # -AL- tentative, doesn't fix the issue.
#                # protobuf::libprotobuf  # -AL- tentative, adds required gtest dependency but doesn't fix the issue.
#                # arrow_compute_testing # -AL- tentative, doesn't fix the issue.
#                ${ODBC_LIBRARIES}
#                ${ODBCINST}
#                ${SQLite3_LIBRARIES}
#                # arrow_odbc_spi_impl_shared  <- -AL- moved to ARROW_FLIGHT_SQL_ODBC_TEST_LINK_LIBS
#                STATIC_LINK_LIBS
#                GTest::gtest
#                GTest::gtest_main
#                # ${ARROW_TEST_LINK_LIBS}
#                ${ARROW_FLIGHT_SQL_ODBC_TEST_LINK_LIBS})


# 1️⃣ Create the test executable with the desired target name
add_executable(arrow-flight-sql-odbc-test
    odbc_test_suite.cc
    odbc_test_suite.h
    columns_test.cc
    connection_attr_test.cc
    connection_info_test.cc
    connection_test.cc
    errors_test.cc
    get_functions_test.cc
    statement_attr_test.cc
    statement_test.cc
    tables_test.cc
    type_info_test.cc
    ${ARROW_FLIGHT_SQL_MOCK_SERVER_SRCS}
)

# 2️⃣ Add compile definitions
target_compile_definitions(arrow-flight-sql-odbc-test PRIVATE
    UNICODE
)

# 3️⃣ Link libraries

# a) Static libraries (e.g., gtest)
target_link_libraries(arrow-flight-sql-odbc-test PRIVATE
    GTest::gtest
    GTest::gtest_main
    ${ARROW_FLIGHT_SQL_ODBC_TEST_LINK_LIBS}
)

# b) Extra libraries (ODBC, SQLite3)
target_link_libraries(arrow-flight-sql-odbc-test PRIVATE
    ${ODBC_LIBRARIES}
    ${ODBCINST}
    ${SQLite3_LIBRARIES}
)

# 4️⃣ Add test to CTest with the same name
add_test(NAME arrow-flight-sql-odbc-test COMMAND arrow-flight-sql-odbc-test)


endif()              

# -AL- local do not commit - Function to print all linked libraries for a target
function(print_linked_libraries target)
    # Get PRIVATE + PUBLIC + INTERFACE
    get_target_property(libs_private ${target} LINK_LIBRARIES)
    get_target_property(libs_interface ${target} INTERFACE_LINK_LIBRARIES)

    message(STATUS "==== Libraries linked to target '${target}' ====")
    
    if(libs_private)
        message(STATUS "PRIVATE/PUBLIC libs: ${libs_private}")
    else()
        message(STATUS "PRIVATE/PUBLIC libs: NONE")
    endif()

    if(libs_interface)
        message(STATUS "INTERFACE libs: ${libs_interface}")
    else()
        message(STATUS "INTERFACE libs: NONE")
    endif()

    message(STATUS "===============================================")
endfunction()

# Usage
print_linked_libraries(arrow-flight-sql-odbc-test)
# print_linked_libraries(flight_sql_odbc_test)
print_linked_libraries(arrow_odbc_spi_impl_shared)

# Disable unity build due to sqlite_sql_info.cc conflict with sql.h and sqlext.h headers.
set_target_properties(arrow-flight-sql-odbc-test PROPERTIES UNITY_BUILD OFF)
