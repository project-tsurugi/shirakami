# Copyright 2019-2026 Project Tsurugi.
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
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} --coverage -fprofile-update=atomic")
endif ()

if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    add_definitions(-DSHIRAKAMI_LINUX)
endif ()

cmake_host_system_information(RESULT cores QUERY NUMBER_OF_LOGICAL_CORES)

if (CMAKE_CXX_COMPILER_ID MATCHES "^(Clang|AppleClang)$")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsized-deallocation")
endif ()

# about index
add_definitions(-DINDEX_YAKUSHIMA)
add_definitions(-DYAKUSHIMA_EPOCH_TIME=40)
message("It uses yakushima as index structure.")

set(logging_set 0)
if (BUILD_PWAL)
    # Begin : Limestone API check
    # For developer convenience, allow integration with both old and new limestone.
    include(CheckCXXSourceCompiles)

    set(CMAKE_REQUIRED_LIBRARIES limestone)
    set(AVAILABLE_LIMESTONE_API_DEFINE "")
    function(check_limestone_api varname method_call)
        set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)  # to avoid troubles around linking ASAN
        check_cxx_source_compiles(
"#include <limestone/api/datastore.h>
int main([[maybe_unused]] int argc, [[maybe_unused]] char *argv[]){
    (void)${method_call};
    return 0;
}"
            ${varname})
        if(${varname})
            set(AVAILABLE_LIMESTONE_API_DEFINE "${AVAILABLE_LIMESTONE_API_DEFINE} -D${varname}=1" PARENT_SCOPE)
        endif()
    endfunction()

    #check_limestone_api(HAVE_LIMESTONE_CONFIG_CTOR_VECBOOSTFSPATH_BOOSTFSPATH
    #                    "limestone::api::configuration{{boost::filesystem::path{\"/tmp\"}}, boost::filesystem::path{\"/tmp\"}}")
    check_limestone_api(HAVE_LIMESTONE_CONFIG_CTOR_NONE
                        "limestone::api::configuration{}")
    check_limestone_api(HAVE_LIMESTONE_CONFIG_SET_DATA_LOCATION_STDFSPATH
                        "[](limestone::api::configuration* conf){conf->set_data_location(std::filesystem::path{\"/tmp\"});}")
    #check_limestone_api(HAVE_LIMESTONE_DATASTORE_CREATE_CHANNEL_BOOSTFSPATH
    #                    "[](limestone::api::datastore* ds){ds->create_channel(boost::filesystem::path{\"/tmp\"});}")
    check_limestone_api(HAVE_LIMESTONE_DATASTORE_CREATE_CHANNEL_NONE
                        "[](limestone::api::datastore* ds){ds->create_channel();}")
    add_definitions(${AVAILABLE_LIMESTONE_API_DEFINE})
    # End : Limestone API check

    if (logging_set)
        message(FATAL_ERROR "It can select only one logging method.")
    endif ()
    add_definitions(-DPWAL)
    if (PWAL_ENABLE_READ_LOG)
        add_definitions(-DPWAL_ENABLE_READ_LOG)
    endif ()
    message("It uses parallel write ahead logging as logging method.")
    set(logging_set 1)
elseif (BUILD_CPR)
    if (logging_set)
        message(FATAL_ERROR "It can select only one logging method.")
    endif ()
    add_definitions(-DCPR)
    message("It uses cpr as logging method.")
    set(logging_set 1)
else ()
    message("It uses no logging method.")
endif ()

if (TSURUGI_FAST_SHUTDOWN)
    add_definitions(-DTSURUGI_FAST_SHUTDOWN_ON)
    message("shutdown shortcut is enabled by default.")
endif ()

# Begin : parameter settings

# Begin : about kvs

add_definitions(-DPROJECT_ROOT=${PROJECT_SOURCE_DIR})

if (NOT DEFINED KVS_MAX_PARALLEL_THREADS)
    add_definitions(-DKVS_MAX_PARALLEL_THREADS=300)
    add_definitions(-DYAKUSHIMA_MAX_PARALLEL_SESSIONS=300)
else ()
    add_definitions(-DKVS_MAX_PARALLEL_THREADS=${KVS_MAX_PARALLEL_THREADS})
    add_definitions(-DYAKUSHIMA_MAX_PARALLEL_SESSIONS=${KVS_MAX_PARALLEL_THREADS})
endif ()

# End : about kvs

# Begin : about cc

if (NOT DEFINED PARAM_EPOCH_TIME)
    add_definitions(-DPARAM_EPOCH_TIME=0)
else ()
    add_definitions(-DPARAM_EPOCH_TIME=${PARAM_EPOCH_TIME})
endif ()

if (NOT DEFINED PARAM_RETRY_READ)
    add_definitions(-DPARAM_RETRY_READ=0)
else ()
    add_definitions(-DPARAM_RETRY_READ=${PARAM_RETRY_READ})
endif ()

if (NOT DEFINED PARAM_SNAPSHOT_EPOCH)
    add_definitions(-DPARAM_SNAPSHOT_EPOCH=25)
else ()
    add_definitions(-DPARAM_SNAPSHOT_EPOCH=${PARAM_SNAPSHOT_EPOCH})
endif ()

# End : about cc

# Begin : about logging

if (NOT DEFINED PARAM_CHECKPOINT_REST_EPOCH)
    add_definitions(-DPARAM_CHECKPOINT_REST_EPOCH=40)
else ()
    add_definitions(-DPARAM_CHECKPOINT_REST_EPOCH=${PARAM_CHECKPOINT_REST_EPOCH})
endif ()

if (NOT DEFINED PARAM_PWAL_LOG_GCOMMIT_THRESHOLD)
    add_definitions(-DPARAM_PWAL_LOG_GCOMMIT_THRESHOLD=1000)
else ()
    add_definitions(-DPARAM_PWAL_LOG_GCOMMIT_THRESHOLD=${PARAM_PWAL_LOG_GCOMMIT_THRESHOLD})
endif ()

# about diff set of cpr
set(cpr_diff_set 0)
if (CPR_DIFF_UM)
    if(cpr_diff_set)
        message(FATAL_ERROR "You should select one method for cpr's diff set.")
    endif()
    set(cpr_diff_set 1)
    add_definitions(-DCPR_DIFF_UM)
    message("It uses std::unoredered_map for cpr's diff set.")
endif()

if (NOT DEFINED PARAM_CPR_DIFF_SET_RESERVE_NUM)
# If you use for practicaly and seek high performance, set big number.
# If you set big number and run ctest, it takes so much time, so default is 0.
    add_definitions(-DPARAM_CPR_DIFF_SET_RESERVE_NUM=0)
else()
    add_definitions(-DPARAM_CPR_DIFF_SET_RESERVE_NUM=${PARAM_CPR_DIFF_SET_RESERVE_NUM})
endif()

# for test bench, todo remove
if (BCC_7)
        add_definitions(-DBCC_7)
endif ()

# End : about logging

# End : parameter settings

function(set_compile_options target_name)
    if (BUILD_STRICT)
        target_compile_options(${target_name}
            PRIVATE -Wall -Wextra -Werror)
    else()
        target_compile_options(${target_name}
            PRIVATE -Wall -Wextra)
    endif()
endfunction(set_compile_options)
