#!/usr/bin/env bash
#
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

set -ex

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <Arrow dir> <build dir> [ctest args ...]"
  exit 1
fi

arrow_dir=${1}; shift
build_dir=${1}/cpp; shift
source_dir=${arrow_dir}/cpp
binary_output_dir=${build_dir}/${ARROW_BUILD_TYPE:-debug}

export ARROW_TEST_DATA=${arrow_dir}/testing/data
export PARQUET_TEST_DATA=${source_dir}/submodules/parquet-testing/data
export LD_LIBRARY_PATH=${ARROW_HOME}/${CMAKE_INSTALL_LIBDIR:-lib}:${LD_LIBRARY_PATH}

# By default, aws-sdk tries to contact a non-existing local ip host
# to retrieve metadata. Disable this so that S3FileSystem tests run faster.
export AWS_EC2_METADATA_DISABLED=TRUE

# Enable memory debug checks if the env is not set already
if [ -z "${ARROW_DEBUG_MEMORY_POOL}" ]; then
  export ARROW_DEBUG_MEMORY_POOL=trap
fi

exclude_tests=()
exclude_tests+=("arrow-io-hdfs-test")
exclude_tests+=("arrow-hdfs-test")
exclude_tests+=("arrow-array-test")
exclude_tests+=("arrow-buffer-test")
exclude_tests+=("arrow-extension-type-test")
exclude_tests+=("arrow-misc-test")
exclude_tests+=("arrow-public-api-test")
exclude_tests+=("arrow-scalar-test")
exclude_tests+=("arrow-type-test")
exclude_tests+=("arrow-table-test")
exclude_tests+=("arrow-tensor-test")
exclude_tests+=("arrow-sparse-tensor-test")
exclude_tests+=("arrow-stl-test")
exclude_tests+=("arrow-generator-test")
exclude_tests+=("arrow-gtest-util-test")
exclude_tests+=("arrow-random-test")
exclude_tests+=("arrow-concatenate-test")
exclude_tests+=("arrow-data-test")
exclude_tests+=("arrow-diff-test")
exclude_tests+=("arrow-c-bridge-test")
exclude_tests+=("arrow-dlpack-test")
exclude_tests+=("arrow-compute-internals-test")
exclude_tests+=("arrow-compute-expression-test")
exclude_tests+=("arrow-compute-row-test")
exclude_tests+=("arrow-compute-scalar-cast-test")
exclude_tests+=("arrow-compute-scalar-type-test")
exclude_tests+=("arrow-compute-scalar-if-else-test")
exclude_tests+=("arrow-compute-scalar-temporal-test")
exclude_tests+=("arrow-compute-scalar-math-test")
exclude_tests+=("arrow-compute-scalar-utility-test")
exclude_tests+=("arrow-compute-vector-test")
exclude_tests+=("arrow-compute-vector-sort-test")
exclude_tests+=("arrow-compute-vector-selection-test")
exclude_tests+=("arrow-compute-vector-swizzle-test")
exclude_tests+=("arrow-compute-aggregate-test")
exclude_tests+=("arrow-compute-kernel-utility-test")
exclude_tests+=("arrow-canonical-extensions-test")
exclude_tests+=("arrow-io-buffered-test")
exclude_tests+=("arrow-io-compressed-test")
exclude_tests+=("arrow-io-file-test")
exclude_tests+=("arrow-io-memory-test")
exclude_tests+=("arrow-utility-test")
exclude_tests+=("arrow-async-utility-test")
exclude_tests+=("arrow-bit-utility-test")
exclude_tests+=("arrow-crc32-test")
exclude_tests+=("arrow-threading-utility-test")
exclude_tests+=("arrow-json-integration-test")
exclude_tests+=("arrow-csv-test")
exclude_tests+=("arrow-acero-plan-test")
exclude_tests+=("arrow-acero-source-node-test")
exclude_tests+=("arrow-acero-fetch-node-test")
exclude_tests+=("arrow-acero-order-by-node-test")
exclude_tests+=("arrow-acero-hash-join-node-test")
exclude_tests+=("arrow-acero-pivot-longer-node-test")
exclude_tests+=("arrow-acero-asof-join-node-test")
exclude_tests+=("arrow-acero-sorted-merge-node-test")
exclude_tests+=("arrow-acero-tpch-node-test")
exclude_tests+=("arrow-acero-union-node-test")
exclude_tests+=("arrow-acero-aggregate-node-test")
exclude_tests+=("arrow-acero-util-test")
exclude_tests+=("arrow-acero-hash-aggregate-test")
exclude_tests+=("arrow-acero-test-util-internal-test")
exclude_tests+=("arrow-dataset-dataset-test")
exclude_tests+=("arrow-dataset-dataset-writer-test")
exclude_tests+=("arrow-dataset-discovery-test")
exclude_tests+=("arrow-dataset-file-ipc-test")
exclude_tests+=("arrow-dataset-file-test")
exclude_tests+=("arrow-dataset-partition-test")
exclude_tests+=("arrow-dataset-scanner-test")
exclude_tests+=("arrow-dataset-subtree-test")
exclude_tests+=("arrow-dataset-write-node-test")
exclude_tests+=("arrow-dataset-file-csv-test")
exclude_tests+=("arrow-dataset-file-json-test")
exclude_tests+=("arrow-dataset-file-parquet-test")
exclude_tests+=("arrow-dataset-file-parquet-encryption-test")
exclude_tests+=("arrow-filesystem-test")
exclude_tests+=("arrow-gcsfs-test")
exclude_tests+=("arrow-s3fs-test")
exclude_tests+=("arrow-flight-internals-test")
exclude_tests+=("arrow-flight-test")
exclude_tests+=("arrow-flight-sql-test")
exclude_tests+=("arrow-feather-test")
exclude_tests+=("arrow-ipc-message-internal-test")
exclude_tests+=("arrow-ipc-read-write-test")
exclude_tests+=("arrow-ipc-tensor-test")
exclude_tests+=("arrow-json-test")
exclude_tests+=("arrow-substrait-substrait-test")
exclude_tests+=("parquet-internals-test")
exclude_tests+=("parquet-encoding-test")
exclude_tests+=("parquet-reader-test")
exclude_tests+=("parquet-writer-test")
exclude_tests+=("parquet-chunker-test")
exclude_tests+=("parquet-arrow-reader-writer-test")
exclude_tests+=("parquet-arrow-internals-test")
exclude_tests+=("parquet-arrow-metadata-test")
exclude_tests+=("parquet-encryption-test")
exclude_tests+=("parquet-encryption-key-management-test")
exclude_tests+=("parquet-file-deserialize-test")
exclude_tests+=("parquet-schema-test")

ctest_options=()
if ! type azurite >/dev/null 2>&1; then
  exclude_tests+=("arrow-azurefs-test")
fi
if ! type storage-testbench >/dev/null 2>&1; then
  exclude_tests+=("arrow-gcsfs-test")
fi
if ! type minio >/dev/null 2>&1; then
  exclude_tests+=("arrow-s3fs-test")
fi
case "$(uname)" in
  Linux)
    n_jobs=$(nproc)
    ;;
  Darwin)
    n_jobs=$(sysctl -n hw.ncpu)
    # TODO: https://github.com/apache/arrow/issues/40410
    exclude_tests+=("arrow-s3fs-test")
    ;;
  MINGW*)
    n_jobs=${NUMBER_OF_PROCESSORS:-1}
    # TODO: Enable these crashed tests.
    # https://issues.apache.org/jira/browse/ARROW-9072
    exclude_tests+=("gandiva-binary-test")
    exclude_tests+=("gandiva-boolean-expr-test")
    exclude_tests+=("gandiva-date-time-test")
    exclude_tests+=("gandiva-decimal-single-test")
    exclude_tests+=("gandiva-decimal-test")
    exclude_tests+=("gandiva-filter-project-test")
    exclude_tests+=("gandiva-filter-test")
    exclude_tests+=("gandiva-hash-test")
    exclude_tests+=("gandiva-if-expr-test")
    exclude_tests+=("gandiva-in-expr-test")
    exclude_tests+=("gandiva-internals-test")
    exclude_tests+=("gandiva-literal-test")
    exclude_tests+=("gandiva-null-validity-test")
    exclude_tests+=("gandiva-precompiled-test")
    exclude_tests+=("gandiva-projector-test")
    exclude_tests+=("gandiva-utf8-test")
    ;;
  *)
    n_jobs=${NPROC:-1}
    ;;
esac
if [ "${#exclude_tests[@]}" -gt 0 ]; then
  IFS="|"
  ctest_options+=(--exclude-regex "${exclude_tests[*]}")
  unset IFS
fi

if [ "${ARROW_EMSCRIPTEN:-OFF}" = "ON" ]; then
  n_jobs=1 # avoid spurious fails on emscripten due to loading too many big executables
fi

pushd "${build_dir}"

if [ -z "${PYTHON}" ] && ! which python > /dev/null 2>&1; then
  export PYTHON="${PYTHON:-python3}"
fi
if [ "${ARROW_USE_MESON:-OFF}" = "ON" ]; then
  ARROW_BUILD_EXAMPLES=OFF # TODO: Remove this
  meson test \
    --max-lines=0 \
    --no-rebuild \
    --print-errorlogs \
    --suite arrow \
    --timeout-multiplier=10 \
    "$@"
else
  ctest \
    --label-regex unittest \
    --output-on-failure \
    --parallel "${n_jobs}" \
    --repeat until-pass:3 \
    --timeout "${ARROW_CTEST_TIMEOUT:-300}" \
    "${ctest_options[@]}" \
    "$@"
fi

# This is for testing find_package(Arrow).
#
# Note that this is not a perfect solution. We should improve this
# later.
#
# * This is ad-hoc
# * This doesn't test other CMake packages such as ArrowDataset
if [ "${ARROW_USE_MESON:-OFF}" = "OFF" ] && \
     [ "${ARROW_EMSCRIPTEN:-OFF}" = "OFF" ] && \
     [ "${ARROW_USE_ASAN:-OFF}" = "OFF" ] && \
     [ "${ARROW_USE_TSAN:-OFF}" = "OFF" ] && \
     [ "${ARROW_CSV:-ON}" = "ON" ]; then
  CMAKE_PREFIX_PATH="${CMAKE_INSTALL_PREFIX:-${ARROW_HOME}}"
  case "$(uname)" in
    MINGW*)
      # <prefix>/lib/cmake/ isn't searched on Windows.
      #
      # See also:
      # https://cmake.org/cmake/help/latest/command/find_package.html#config-mode-search-procedure
      CMAKE_PREFIX_PATH+="/lib/cmake/"
      ;;
  esac
  if [ -n "${VCPKG_ROOT}" ] && [ -n "${VCPKG_DEFAULT_TRIPLET}" ]; then
    CMAKE_PREFIX_PATH+=";${VCPKG_ROOT}/installed/${VCPKG_DEFAULT_TRIPLET}"
  fi
  cmake \
    -S "${source_dir}/examples/minimal_build" \
    -B "${build_dir}/examples/minimal_build" \
    -DCMAKE_PREFIX_PATH="${CMAKE_PREFIX_PATH}"
  cmake --build "${build_dir}/examples/minimal_build"
  pushd "${source_dir}/examples/minimal_build"
  # PATH= is for Windows.
  PATH="${CMAKE_INSTALL_PREFIX:-${ARROW_HOME}}/bin:${PATH}" \
    "${build_dir}/examples/minimal_build/arrow-example"
  popd
fi

if [ "${ARROW_BUILD_EXAMPLES}" == "ON" ]; then
    examples=$(find "${binary_output_dir}" -executable -name "*example")
    if [ "${examples}" == "" ]; then
        echo "=================="
        echo "No examples found!"
        echo "=================="
        exit 1
    fi
    for ex in ${examples}
    do
        echo "=================="
        echo "Executing ${ex}"
        echo "=================="
        ${ex}
    done
fi

if [ "${ARROW_FUZZING}" == "ON" ]; then
    # Fuzzing regression tests
    # Some fuzz regression files may trigger huge memory allocations,
    # let the allocator return null instead of aborting.
    export ASAN_OPTIONS="$ASAN_OPTIONS allocator_may_return_null=1"
    "${binary_output_dir}/arrow-ipc-stream-fuzz" "${ARROW_TEST_DATA}"/arrow-ipc-stream/crash-*
    "${binary_output_dir}/arrow-ipc-stream-fuzz" "${ARROW_TEST_DATA}"/arrow-ipc-stream/*-testcase-*
    "${binary_output_dir}/arrow-ipc-file-fuzz" "${ARROW_TEST_DATA}"/arrow-ipc-file/*-testcase-*
    "${binary_output_dir}/arrow-ipc-tensor-stream-fuzz" "${ARROW_TEST_DATA}"/arrow-ipc-tensor-stream/*-testcase-*
    if [ "${ARROW_PARQUET}" == "ON" ]; then
      "${binary_output_dir}/parquet-arrow-fuzz" "${ARROW_TEST_DATA}"/parquet/fuzzing/*-testcase-*
    fi
    "${binary_output_dir}/arrow-csv-fuzz" "${ARROW_TEST_DATA}"/csv/fuzzing/*-testcase-*
fi

popd
