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

file(GLOB TEST_SOURCES
        "concurrency_control/common/delete/*.cpp"
        "concurrency_control/common/delete_insert/*.cpp"
        "concurrency_control/common/delete_insert_search/*.cpp"
        "concurrency_control/common/delete_insert_scan/*.cpp"
        "concurrency_control/common/delete_scan_upsert/*.cpp"
        "concurrency_control/common/delete_search/*.cpp"
        "concurrency_control/common/delete_update/*.cpp"
        "concurrency_control/common/delete_upsert/*.cpp"
        "concurrency_control/common/helper/*.cpp"
        "concurrency_control/common/insert/*.cpp"
        "concurrency_control/common/insert_update/*.cpp"
        "concurrency_control/common/insert_upsert/*.cpp"
        "concurrency_control/common/upsert/*.cpp"
        "concurrency_control/common/update/*.cpp"
        "concurrency_control/common/scan/*.cpp"
        "concurrency_control/common/scan_upsert/*.cpp"
        "concurrency_control/common/search/*.cpp"
        "concurrency_control/common/search_update/*.cpp"
        "concurrency_control/common/search_upsert/*.cpp"
        "concurrency_control/common/storage/*.cpp"
        )

if (NOT BUILD_WP)
file(GLOB SILO_TEST_SOURCES
        "bench/*.cpp"
        "complicated_test/phantom_protection_test.cpp"
        "concurrency_control/silo/*.cpp"
        "concurrency_control/silo/interface/update/*.cpp"
        "concurrency_control/silo/scan/read_only/*.cpp"
        "sequence/*.cpp"
        "third_party/*.cpp"
        )
        set(TEST_SOURCES ${TEST_SOURCES} ${SILO_TEST_SOURCES})
else ()
file(GLOB WP_TEST_SOURCES
        "concurrency_control/wp/*.cpp"
        "concurrency_control/wp/anomaly/write_crown/*.cpp"
        "concurrency_control/wp/anomaly/write_skew/*.cpp"
        "concurrency_control/wp/interface/*.cpp"
        "concurrency_control/wp/interface/helper/*.cpp"
        "concurrency_control/wp/interface/insert/*.cpp"
        "concurrency_control/wp/interface/search_upsert/*.cpp"
        "concurrency_control/wp/interface/upsert/*.cpp"
        "concurrency_control/wp/wp_basic/*.cpp"
        )
        set(TEST_SOURCES ${TEST_SOURCES} ${WP_TEST_SOURCES})
endif()

if (BUILD_PWAL)
    file(GLOB FAULT_TOLERANCE_TESTS "fault_tolerance/pwal_test.cpp")
    set(TEST_SOURCES ${TEST_SOURCES} ${FAULT_TOLERANCE_TESTS})
endif ()

if (BUILD_CPR)
    file(GLOB FAULT_TOLERANCE_TESTS "fault_tolerance/cpr/*.cpp")
    set(TEST_SOURCES ${TEST_SOURCES} ${FAULT_TOLERANCE_TESTS})
endif ()

register_tests(
        TARGET shirakami
        DEPENDS
          shirakami-impl
          PRIVATE glog::glog
          PRIVATE ${tbb_prefix}tbb
          PRIVATE ${tbb_prefix}tbbmalloc
          PRIVATE ${tbb_prefix}tbbmalloc_proxy
        SOURCES ${TEST_SOURCES}
)
