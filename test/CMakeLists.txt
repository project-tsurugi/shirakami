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
        "cliTest.cpp"
        "clockTest.cpp"
        "errorTest.cpp"
        "logTest.cpp"
        "memoryTest.cpp"
        "recordTest.cpp"
        "schemeTest.cpp"
        "simpleTest.cpp"
        "threadInfoTest.cpp"
        )

if (BUILD_INDEX_KOHLER_MASSTREE)
    file(GLOB INDEX_TEST_SOURCES "masstreeBetaTest.cpp")
    set(TEST_SOURCES ${TEST_SOURCES} ${INDEX_TEST_SOURCES})
endif ()

register_tests(
        TARGET shirakami
        DEPENDS shirakami-impl
        SOURCES ${TEST_SOURCES}
)
