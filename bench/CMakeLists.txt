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

file(GLOB YCSB_SOURCES
  "ycsb.cc"
  "masstree_build.cc"
  "result.cc"
)

file(GLOB MASSTREE_SOURCES
  "masstree.cc"
  "masstree_build.cc"
  "result.cc"
)

add_executable(ycsb
  ${YCSB_SOURCES}
)

clang_format(ycsb)

add_executable(masstree
  ${MASSTREE_SOURCES}
)

clang_format(masstree)

target_link_libraries(ycsb
  PRIVATE kvs_charkey
  PRIVATE kvs_charkey-impl
  PRIVATE Threads::Threads
  PRIVATE gflags::gflags
)

target_include_directories(ycsb
  PRIVATE .
  PRIVATE ./include
  PRIVATE ./../bench/
  PRIVATE ./../include
  PRIVATE ./../src/include
  PRIVATE ./../test/include
  PRIVATE ${gflags_INCLUDE_DIR}
)
target_link_libraries(masstree
  PRIVATE kvs_charkey
  PRIVATE kvs_charkey-impl
  PRIVATE Threads::Threads
  PRIVATE gflags::gflags
)

target_include_directories(masstree
  PRIVATE .
  PRIVATE ./include
  PRIVATE ./../bench/
  PRIVATE ./../include
  PRIVATE ./../src/include
  PRIVATE ./../test/include
  PRIVATE ${gflags_INCLUDE_DIR}
)
