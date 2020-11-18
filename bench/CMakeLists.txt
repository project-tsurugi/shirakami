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
        PRIVATE shirakami
        PRIVATE shirakami-impl
        PRIVATE Threads::Threads
        PRIVATE gflags::gflags
        Boost::filesystem
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

if (BUILD_INDEX_KOHLER_MASSTREE)
    file(GLOB MASSTREE_SOURCES
            "masstree_beta.cpp"
            "build_db.cpp"
            "result.cpp"
            )

    add_executable(masstree
            ${MASSTREE_SOURCES}
            )


    target_link_libraries(masstree
            PRIVATE shirakami
            PRIVATE shirakami-impl
            PRIVATE Threads::Threads
            PRIVATE gflags::gflags
            )

    target_include_directories(masstree
            PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
            PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
            PRIVATE ${PROJECT_SOURCE_DIR}/bench/
            PRIVATE ${PROJECT_SOURCE_DIR}/include
            PRIVATE ${PROJECT_SOURCE_DIR}/src
            PRIVATE ${PROJECT_SOURCE_DIR}/src/include
            PRIVATE ${PROJECT_SOURCE_DIR}/test/include
            PRIVATE ${PROJECT_SOURCE_DIR}/third_party
            PRIVATE ${gflags_INCLUDE_DIR}
            )
endif ()
