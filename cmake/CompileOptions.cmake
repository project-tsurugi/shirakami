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

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fno-omit-frame-pointer")

set(sanitizers "address")
if(ENABLE_UB_SANITIZER)
    # NOTE: UB check requires instrumented libstdc++
    set(sanitizers "${sanitizers},undefined")
endif()
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # do nothing for gcc
elseif(CMAKE_CXX_COMPILER_ID MATCHES "^(Clang|AppleClang)$")
    set(sanitizers "${sanitizers},nullability")
else()
    message(FATAL_ERROR "unsupported compiler ${CMAKE_CXX_COMPILER_ID}")
endif()

if(ENABLE_SANITIZER)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=${sanitizers}")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-sanitize=alignment")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-sanitize-recover=${sanitizers}")
endif()
if(ENABLE_COVERAGE)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} --coverage")
endif()

if(CMAKE_SYSTEM_NAME MATCHES "Linux")
  add_definitions(-DKVS_Linux)
endif()

function(set_compile_options target_name)
#    target_compile_options(${target_name}
#        PRIVATE -Wall -Wextra -Werror)
endfunction(set_compile_options)
