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
        "cc/silo_variant/log_test.cpp"
        "cc/silo_variant/record_test.cpp"
        "cc/silo_variant/thread_info_test.cpp"
        "cc/silo_variant/tid_test.cpp"
        "clock_test.cpp"
        "complicated_test/client_test.cpp"
        "complicated_test/scan_perf_test.cpp"
        "error_test.cpp"
        "interface/helper.cpp"
        "interface/insert_delete_test.cpp"
        "interface/scan_search_test.cpp"
        "interface/simple_delete_test.cpp"
        "interface/simple_insert_test.cpp"
        "interface/simple_scan_test.cpp"
        "interface/simple_search_test.cpp"
        "interface/simple_update_test.cpp"
        "memory_test.cpp"
        "scheme_test.cpp"
        )

if (BUILD_INDEX_KOHLER_MASSTREE)
    file(GLOB INDEX_TEST_SOURCES "masstree_beta_test.cpp")
    set(TEST_SOURCES ${TEST_SOURCES} ${INDEX_TEST_SOURCES})
endif ()

register_tests(
        TARGET shirakami
        DEPENDS shirakami-impl
        SOURCES ${TEST_SOURCES}
)
