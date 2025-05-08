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

cmake_minimum_required(VERSION 3.10)

project(shirakami
        VERSION 1.0.0
        DESCRIPTION "shirakami"
        LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

option(BUILD_BENCHMARK "Build benchmark programs" ON)
option(BUILD_TESTS "Build test programs" ON)
option(BUILD_ONLY_WD_TESTS "Build test programs" OFF)
option(BUILD_DOCUMENTS "Build documents" ON)

option(ENABLE_SANITIZER "enable sanitizer on debug build" ON)
option(ENABLE_UB_SANITIZER "enable undefined behavior sanitizer on debug build" OFF)
option(ENABLE_COVERAGE "enable coverage on debug build" OFF)
option(BUILD_SHARED_LIBS "build shared libraries instead of static" ON)
option(BUILD_STRICT "build with option strictly determine of success" ON)

# transaction processing logic
option(BUILD_PWAL "Build parallel write ahead logging as logging." ON)
option(PWAL_ENABLE_READ_LOG "PWAL log read (/ write) informations to verify whether committed schedule is valid." OFF)

# shutdown shortcut
option(TSURUGI_FAST_SHUTDOWN "enable shortcuts during shutdown process by default" OFF)

# benchmark
option(BUILD_ROCKSDB_BENCH "Build rocksdb benchmark." OFF)

option(FORMAT_FILES_WITH_CLANG_FORMAT_BEFORE_EACH_BUILD "If the command clang-format is available, format source files before each build. Turn this off if the build time is too slow." OFF)

if (FORCE_INSTALL_RPATH)
    message(DEPRECATION "FORCE_INSTALL_RPATH is obsoleted")
endif (FORCE_INSTALL_RPATH)

if (BUILD_PWAL)
  find_package(limestone REQUIRED)
endif()
find_package(yakushima REQUIRED)

option(BUILD_ONLY_WD_BENCH "Build only working directory about bench for dev" OFF)

find_package(Boost
        COMPONENTS filesystem)
find_package(Doxygen)
find_package(TBB
  COMPONENTS tbb tbbmalloc tbbmalloc_proxy
  CONFIG QUIET
)
if (DEFINED TBB_CONFIG)
  set(tbb_prefix "TBB::")
else()
  find_package(TBB REQUIRED)
  set(tbb_prefix "")
endif()
find_package(Threads REQUIRED)
find_package(gflags REQUIRED)
find_package(glog REQUIRED)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CompileOptions)
include(InstallOptions)
include(Tests)
include(IncludeWhatYouUse)

add_library(api INTERFACE)

target_include_directories(api
        INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/shirakami>
        )

set(export_name "shirakami")
set(package_name "shirakami")

configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/cmake/Config.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config.cmake
        @ONLY
)

write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config-version.cmake"
        COMPATIBILITY SameMajorVersion
)

install_custom(api ${export_name})

install(
        FILES
        ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config-version.cmake
        DESTINATION
        ${CMAKE_INSTALL_LIBDIR}/cmake/${package_name}
)

install(
        EXPORT ${package_name}
        NAMESPACE ${package_name}-
        FILE ${package_name}-targets.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${package_name}
        EXPORT_LINK_INTERFACE_LIBRARIES
)

add_subdirectory(src)
add_subdirectory(third_party)

if (BUILD_BENCHMARK)
    add_subdirectory(bench)
endif ()

if (BUILD_TESTS)
    enable_testing()
    add_subdirectory(test)
endif ()

if (BUILD_DOCUMENTS)
    add_subdirectory(doxygen)
endif ()
