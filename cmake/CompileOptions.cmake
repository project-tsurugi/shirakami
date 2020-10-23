# Copyright 2019-2019 tsurugi project.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fno-omit-frame-pointer")

set(sanitizers "address")
if (ENABLE_UB_SANITIZER)
    # NOTE: UB check requires instrumented libstdc++
    set(sanitizers "${sanitizers},undefined")
endif ()
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # do nothing for gcc
elseif (CMAKE_CXX_COMPILER_ID MATCHES "^(Clang|AppleClang)$")
    set(sanitizers "${sanitizers},nullability")
else ()
    message(FATAL_ERROR "unsupported compiler ${CMAKE_CXX_COMPILER_ID}")
endif ()

if (ENABLE_SANITIZER)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=${sanitizers}")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-sanitize=alignment")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-sanitize-recover=${sanitizers}")
endif ()
if (ENABLE_COVERAGE)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} --coverage")
endif ()

if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    add_definitions(-DSHIRAKAMI_LINUX)
endif ()

set(cc_set 0)
if (BUILD_CC_SILO_VARIANT)
    if (cc_set)
        message(FATAL_EEROR "It can select only one cc protocol.")
    endif ()
    add_definitions(-DCC_SILO_VARIANT)
    message("It uses silo variant cc as concurrency control protocols.")
    set(cc_set 1)
endif ()

set(index_set 0)
if (BUILD_INDEX_KOHLER_MASSTREE)
    if (index_set)
        message(FATAL_ERROR "It can select only one index structure.")
    endif ()
    add_definitions(-DINDEX_KOHLER_MASSTREE)
    message("It uses kohler masstree as index structure.")
    set(index_set 1)
elseif (BUILD_INDEX_YAKUSHIMA)
    if (index_set)
        message(FATAL_ERROR "It can select only one index structure.")
    endif ()
    add_definitions(-DINDEX_YAKUSHIMA)
    add_definitions(-DYAKUSHIMA_MAX_PARALLEL_SESSIONS=250)
    add_definitions(-DYAKUSHIMA_EPOCH_TIME=40)
    message("It uses yakushima as index structure.")
    set(index_set 1)
endif ()

cmake_host_system_information(RESULT cores QUERY NUMBER_OF_LOGICAL_CORES)
add_definitions(-DKVS_EPOCH_TIME=40)
add_definitions(-DKVS_NUMBER_OF_LOGICAL_CORES=${cores})
add_definitions(-DKVS_MAX_PARALLEL_THREADS=500)
add_definitions(-DKVS_MAX_KEY_LENGTH=1000)
add_definitions(-DKVS_LOG_GC_THRESHOLD=1)
add_definitions(-DPROJECT_ROOT=${PROJECT_SOURCE_DIR})

if (BUILD_PWAL)
    if (BUILD_CPR)
        message(FATAL_ERROR "It can select only one logging method.")
    endif ()
    add_definitions(-DPWAL)
elseif (BUILD_CPR)
    add_definitions(-DCPR)
endif ()

function(set_compile_options target_name)
    target_compile_options(${target_name}
            PRIVATE -Wall -Wextra -Werror)
endfunction(set_compile_options)
