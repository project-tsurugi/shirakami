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
        "ycsb.cpp"
        "build_db.cpp"
        "result.cpp"
        )

add_executable(ycsb
        ${YCSB_SOURCES}
        )

target_link_libraries(ycsb
        PRIVATE gflags::gflags
        PRIVATE glog::glog
        PRIVATE shirakami
        PRIVATE shirakami-impl
        PRIVATE tsl::hopscotch_map
        PRIVATE tbb
        PRIVATE tbbmalloc
        PRIVATE tbbmalloc_proxy
        PRIVATE Boost::filesystem
        PRIVATE Threads::Threads
        )

target_include_directories(ycsb
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/bench/
        PRIVATE ${PROJECT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/src
        PRIVATE ${PROJECT_SOURCE_DIR}/src/include
        PRIVATE ${PROJECT_SOURCE_DIR}/test/include
        PRIVATE ${PROJECT_SOURCE_DIR}/third_party
        PRIVATE ${PROJECT_SOURCE_DIR}/third_party/msgpack-c/include
        PRIVATE ${gflags_INCLUDE_DIR}
        )

add_test(
        NAME ycsb
        COMMAND ycsb)

file(GLOB ROCKSDB_SOURCES
        "rocksdb_bench.cpp"
        )

if (BUILD_ROCKSDB_BENCH)
    add_executable(rocksdb_bench
            ${ROCKSDB_SOURCES}
            )

    target_link_libraries(rocksdb_bench
            PRIVATE glog::glog
            PRIVATE gflags::gflags
            PRIVATE Threads::Threads
            PRIVATE librocksdb.so
            )

    target_include_directories(rocksdb_bench
            PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
            PRIVATE ${PROJECT_SOURCE_DIR}/src/include
            PRIVATE ${gflags_INCLUDE_DIR}
            )
endif ()

file(GLOB MAP_SOURCES
        "map.cpp"
        )

add_executable(map
        ${MAP_SOURCES}
        )

target_include_directories(map
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/src/include
        PRIVATE ${gflags_INCLUDE_DIR}
        )

target_link_libraries(map
        PRIVATE glog::glog
        PRIVATE gflags::gflags
        PRIVATE tsl::hopscotch_map
        PRIVATE Threads::Threads
        )
