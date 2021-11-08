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
if (NOT BUILD_WP)
file(GLOB TEST_SOURCES
        "bench/*.cpp"
        "complicated_test/phantom_protection_test.cpp"
        "concurrency_control/common/helper/*.cpp"
        "concurrency_control/common/search_upsert/*.cpp"
        "concurrency_control/silo/*.cpp"
        "concurrency_control/silo/delete/*.cpp"
        "concurrency_control/silo/insert/*.cpp"
        "concurrency_control/silo/read_write/*.cpp"
        "concurrency_control/silo/storage/*.cpp"
        "concurrency_control/silo/scan/*.cpp"
        "concurrency_control/silo/update/*.cpp"
        "concurrency_control/silo/upsert/*.cpp"
        "sequence/*.cpp"
        "third_party/*.cpp"
        )
else ()
file(GLOB TEST_SOURCES
        "concurrency_control/common/helper/*.cpp"
        "concurrency_control/common/search_upsert/*.cpp"
        "concurrency_control/wp/*.cpp"
        "concurrency_control/wp/interface/helper/*.cpp"
        "concurrency_control/wp/interface/search_upsert/*.cpp"
        "concurrency_control/wp/interface/upsert/*.cpp"
        )

endif()

if (BUILD_PWAL)
    file(GLOB FAULT_TOLERANCE_TESTS "fault_tolerance/pwal_test.cpp")
    set(TEST_SOURCES ${TEST_SOURCES} ${FAULT_TOLERANCE_TESTS})
endif ()

if (BUILD_CPR)
    file(GLOB FAULT_TOLERANCE_TESTS "fault_tolerance/cpr_test.cpp")
    set(TEST_SOURCES ${TEST_SOURCES} ${FAULT_TOLERANCE_TESTS})
endif ()

register_tests(
        TARGET shirakami
        DEPENDS shirakami-impl
        SOURCES ${TEST_SOURCES}
)
