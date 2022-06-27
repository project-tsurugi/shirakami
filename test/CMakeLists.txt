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


file(GLOB WP_TEST_SOURCES
        "concurrency_control/*.cpp"
        "concurrency_control/anomaly/write_crown/*.cpp"
        "concurrency_control/anomaly/write_skew/*.cpp"
        "concurrency_control/hybrid/delete_update/*.cpp"
        "concurrency_control/hybrid/insert/*.cpp"
        "concurrency_control/hybrid/search_wp/*.cpp"
        "concurrency_control/hybrid/scan_wp/*.cpp"
        "concurrency_control/hybrid/scan_upsert/*.cpp"
        "concurrency_control/hybrid/search_upsert/*.cpp"
        "concurrency_control/long_tx/*.cpp"
        "concurrency_control/long_tx/delete/*.cpp"
        "concurrency_control/long_tx/delete_insert/*.cpp"
        "concurrency_control/long_tx/insert/*.cpp"
        "concurrency_control/long_tx/helper/*.cpp"
        "concurrency_control/long_tx/search/*.cpp"
        "concurrency_control/long_tx/scan/*.cpp"
        "concurrency_control/long_tx/scan/open_scan/*.cpp"
        "concurrency_control/long_tx/scan/next/*.cpp"
        "concurrency_control/long_tx/scan_upsert/*.cpp"
        "concurrency_control/long_tx/scan_wp/*.cpp"
        "concurrency_control/long_tx/search_upsert/*.cpp"
        "concurrency_control/long_tx/update/*.cpp"
        "concurrency_control/long_tx/upsert/*.cpp"
        "concurrency_control/long_tx/termination/*.cpp"
        "concurrency_control/long_tx/tx_state/*.cpp"
        "concurrency_control/long_tx/wp_basic/*.cpp"
        "concurrency_control/long_tx_or_hybrid/interface/upsert/*.cpp"
        "concurrency_control/read_only_tx/*.cpp"
        "concurrency_control/read_only_tx/scan/*.cpp"
        "concurrency_control/read_only_tx/scan/open_scan/*.cpp"
        "concurrency_control/read_only_tx/scan/next/*.cpp"
        "concurrency_control/short_tx/*.cpp"
        "concurrency_control/short_tx/delete/*.cpp"
        "concurrency_control/short_tx/delete_insert/*.cpp"
        "concurrency_control/short_tx/delete_insert_search/*.cpp"
        "concurrency_control/short_tx/delete_insert_scan/*.cpp"
        "concurrency_control/short_tx/delete_scan_upsert/*.cpp"
        "concurrency_control/short_tx/delete_search/*.cpp"
        "concurrency_control/short_tx/delete_update/*.cpp"
        "concurrency_control/short_tx/delete_upsert/*.cpp"
        "concurrency_control/short_tx/helper/*.cpp"
        "concurrency_control/short_tx/insert/*.cpp"
        "concurrency_control/short_tx/insert_delete/*.cpp"
        "concurrency_control/short_tx/insert_update/*.cpp"
        "concurrency_control/short_tx/insert_upsert/*.cpp"
        "concurrency_control/short_tx/upsert/*.cpp"
        "concurrency_control/short_tx/update/*.cpp"
        "concurrency_control/short_tx/scan/*.cpp"
        "concurrency_control/short_tx/scan/next/*.cpp"
        "concurrency_control/short_tx/scan/open_scan/*.cpp"
        "concurrency_control/short_tx/scan_upsert/*.cpp"
        "concurrency_control/short_tx/search/*.cpp"
        "concurrency_control/short_tx/search_update/*.cpp"
        "concurrency_control/short_tx/search_upsert/*.cpp"
        "concurrency_control/short_tx/search_upsert/multi_thread/*.cpp"
        "concurrency_control/short_tx/storage/*.cpp"
        "concurrency_control/short_tx/termination/*.cpp"
        "concurrency_control/short_tx/tx_state/*.cpp"
        "start/*.cpp"
        "shutdown/*.cpp"
        )
        set(TEST_SOURCES ${TEST_SOURCES} ${WP_TEST_SOURCES})

if (BUILD_PWAL)
  file (GLOB LOGGING_TEST_SOURCES
        "datastore/*.cpp"
        )
  set(TEST_SOURCES ${TEST_SOURCES} ${LOGGING_TEST_SOURCES})
endif()

if (BUILD_PWAL)
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
else()
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
endif()
