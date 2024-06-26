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

option(ENABLE_INCLUDE_WHAT_YOU_USE "Enable include-what-you-use. Use include-what-you-use to sanitize header-include dependencies. Using include-what-you-use can slow the compilation, so if the compilation is slow turn this off." OFF)

if (ENABLE_INCLUDE_WHAT_YOU_USE)
    find_program(INCLUDE_WHAT_YOU_USE include-what-you-use)
    if(INCLUDE_WHAT_YOU_USE)
        message(STATUS "Include What You Use")
        set(CMAKE_CXX_INCLUDE_WHAT_YOU_USE include-what-you-use)
    else()
        message(WARNING "Could not find include-what-you-use")
    endif()
endif()
