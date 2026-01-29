#!/usr/bin/env bash

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

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
echo "SOURCE=$SOURCE"

SCRIPT_DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
echo "SCRIPT_DIR=$SCRIPT_DIR"

REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
echo "REPO_DIR=$REPO_DIR"

ODBC_RELEASE_BUILD_DIR="$REPO_DIR/build/cpp/release"
echo "ODBC_RELEASE_BUILD_DIR=$ODBC_RELEASE_BUILD_DIR"

ODBC_DEBUG_BUILD_DIR="$REPO_DIR/build/cpp/debug"
echo "ODBC_DEBUG_BUILD_DIR=$ODBC_DEBUG_BUILD_DIR"

# ODBC_LIB_FILENAME="$ODBC_DEBUG_BUILD_DIR/libarrow_flight_sql_odbc.dylib"
# echo "ODBC_LIB_FILENAME=$ODBC_LIB_FILENAME"

ODBC_LIB_FILENAME="$ODBC_RELEASE_BUILD_DIR/libarrow_flight_sql_odbc.dylib"
echo "ODBC_LIB_FILENAME=$ODBC_LIB_FILENAME"

if [ ! -f "$ODBC_LIB_FILENAME" ]
then
  echo "Cannot find ODBC library file: $ODBC_LIB_FILENAME"
  exit 1
fi

echo "[ODBC Drivers]"                                          > "$REPO_DIR/arrow-odbc-install.ini"
echo "Apache Arrow Flight SQL ODBC Driver=Installed"          >> "$REPO_DIR/arrow-odbc-install.ini"
echo                                                          >> "$REPO_DIR/arrow-odbc-install.ini"
echo "[Apache Arrow Flight SQL ODBC Driver]"                  >> "$REPO_DIR/arrow-odbc-install.ini"
echo "Description=An ODBC Driver for Apache Arrow Flight SQL" >> "$REPO_DIR/arrow-odbc-install.ini"
echo "Driver=$ODBC_LIB_FILENAME"                              >> "$REPO_DIR/arrow-odbc-install.ini"
echo "Setup=$ODBC_LIB_FILENAME"                               >> "$REPO_DIR/arrow-odbc-install.ini"

ARM_LIBIODBC_PATH="/opt/homebrew/opt/libiodbc/lib"
INTEL_LIBIODBC_PATH="/usr/local/opt/libiodbc/"

export ODBCINSTINI="$REPO_DIR/arrow-odbc-install.ini"
echo "Exported ODBCINSTINI=$ODBCINSTINI"
export DYLD_LIBRARY_PATH=$ODBC_LIB_PATH:$ARM_LIBIODBC_PATH:$INTEL_LIBIODBC_PATH:$DYLD_LIBRARY_PATH
echo "Exported DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH"
