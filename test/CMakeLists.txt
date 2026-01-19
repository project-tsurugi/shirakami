# Copyright 2019-2025 tsurugi project.
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

if (BUILD_ONLY_WD_TESTS)
  message("Build only working directory about tests.")
  file(GLOB CC_TEST_SOURCES
        "wd/*.cpp"
        )
else ()
  file(GLOB CC_TEST_SOURCES
          "concurrency_control/*.cpp"
          "concurrency_control/anomaly/Visio-TestCase/*.cpp"
          "concurrency_control/anomaly/Visio-TestCase_Functions_v1/false_positive/*.cpp"
          "concurrency_control/anomaly/Visio-TestCase_ReadCrownv1/*.cpp"
          "concurrency_control/anomaly/Visio-TestCase_WriteCrownv3/*.cpp"
          "concurrency_control/bg_work/*.cpp"
          "concurrency_control/complicated/*.cpp"
          "concurrency_control/read_only_tx/*.cpp"
          "concurrency_control/read_only_tx/search/*.cpp"
          "concurrency_control/read_only_tx/scan/*.cpp"
          "concurrency_control/read_only_tx/scan/open_scan/*.cpp"
          "concurrency_control/read_only_tx/scan/next/*.cpp"
          "concurrency_control/read_only_tx/tx_state/*.cpp"
          "concurrency_control/short_tx/*.cpp"
          "concurrency_control/short_tx/delete/*.cpp"
          "concurrency_control/short_tx/delete_insert/*.cpp"
          "concurrency_control/short_tx/delete_insert_scan/*.cpp"
          "concurrency_control/short_tx/delete_insert_search/*.cpp"
          "concurrency_control/short_tx/delete_insert_search_upsert/*.cpp"
          "concurrency_control/short_tx/delete_insert_search_upsert_scan/*.cpp"
          "concurrency_control/short_tx/delete_scan/*.cpp"
          "concurrency_control/short_tx/delete_scan_upsert/*.cpp"
          "concurrency_control/short_tx/delete_search/*.cpp"
          "concurrency_control/short_tx/delete_update/*.cpp"
          "concurrency_control/short_tx/delete_upsert/*.cpp"
          "concurrency_control/short_tx/helper/*.cpp"
          "concurrency_control/short_tx/insert/*.cpp"
          "concurrency_control/short_tx/insert_update/*.cpp"
          "concurrency_control/short_tx/insert_upsert/*.cpp"
          "concurrency_control/short_tx/insert_scan/*.cpp"
          "concurrency_control/short_tx/insert_search/*.cpp"
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
          "database_options/*.cpp"
          "misc_ut/*.cpp"
          "result/*.cpp"
          "start/*.cpp"
          "shirakami_issues/*.cpp"
          "shutdown/*.cpp"
          "sequence/*.cpp"
          "storage/*.cpp"
          "tid_word/*.cpp"
          "transaction_options/*.cpp"
          "tsurugi_issues/*.cpp"
          "tx_id/*.cpp"
          )

  file (GLOB WAITING_BYPASS_TEST_SOURCES
  )

  if (BUILD_PWAL)
    file (GLOB LOGGING_TEST_SOURCES
          "datastore/*.cpp"
          "datastore/single_recovery/*.cpp"
          "datastore/two_recovery/*.cpp"
          "datastore/three_recovery/*.cpp"
          )
    file (GLOB TEST_WITH_LIMESTONE_DOUBLE_SOURCES
          "test_double/*.cpp"
          )
  endif()
endif()

file (GLOB LIMESTONE_DOUBLE_SOURCES "test_double/doubles/limestone_double.cpp")
add_library(limestone-double ${LIMESTONE_DOUBLE_SOURCES})
target_link_libraries(limestone-double PRIVATE limestone)
target_include_directories(limestone-double
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
        )

register_tests(
        TARGET shirakami
        DEPENDS
          shirakami-impl
          PRIVATE glog::glog
          PRIVATE ${tbb_prefix}tbb
          PRIVATE ${tbb_prefix}tbbmalloc
          PRIVATE ${tbb_prefix}tbbmalloc_proxy
          PRIVATE yakushima
          PRIVATE atomic
        SOURCES ${CC_TEST_SOURCES}
        TEST_LABELS "CC"
)

if (BUILD_ONLY_WD_TESTS)
else ()
  if (BUILD_PWAL)
    register_tests(
            TARGET shirakami
            DEPENDS
              shirakami-impl
              PRIVATE glog::glog
              PRIVATE ${tbb_prefix}tbb
              PRIVATE ${tbb_prefix}tbbmalloc
              PRIVATE ${tbb_prefix}tbbmalloc_proxy
              PRIVATE yakushima
              PRIVATE atomic
            SOURCES ${LOGGING_TEST_SOURCES}
            TEST_LABELS "LOGGING"
    )
    register_tests(
            TARGET shirakami
            DEPENDS
              shirakami-impl
              PRIVATE glog::glog
              PRIVATE ${tbb_prefix}tbb
              PRIVATE ${tbb_prefix}tbbmalloc
              PRIVATE ${tbb_prefix}tbbmalloc_proxy
              PRIVATE yakushima
              PRIVATE atomic
              PRIVATE limestone-double
            SOURCES ${TEST_WITH_LIMESTONE_DOUBLE_SOURCES}
            TEST_LABELS "LOGGING"
    )
  endif()
endif ()
