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

file(GLOB SOURCES
  "*.cc"
  "../test/result.cc"
)

add_executable(ycsb
  ${SOURCES}
)

find_package(Threads REQUIRED)
find_package(gflags REQUIRED)
find_library(gflags_LIBRARY_FILE NAMES gflags)
find_path(gflags_INCLUDE_DIR NAMES gflags/gflags.h)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(gflags DEFAULT_MSG
  gflags_LIBRARY_FILE
  gflags_INCLUDE_DIR)

if(gflags_LIBRARY_FILE AND gflags_INCLUDE_DIR)
  set(gflags_FOUND ON)
  add_library(gflags::gflags SHARED IMPORTED)
  set_target_properties(gflags::gflags PROPERTIES
    IMPORTED_LOCATION "${gflags_LIBRARY_FILE}"
    INTERFACE_INCLUDE_DIRECTORIES "${gflags_INCLUDE_DIR}")
else()
  set(gflags_FOUND OFF)
endif()

target_link_libraries(ycsb
  PRIVATE kvs_charkey
  PRIVATE kvs_charkey-impl
  PRIVATE Threads::Threads
  PRIVATE gflags::gflags
)

target_include_directories(ycsb
  PRIVATE .
  PRIVATE ./../include
  PRIVATE ./../test/
  PRIVATE ./../src
  PRIVATE ${gflags_INCLUDE_DIR}
)
