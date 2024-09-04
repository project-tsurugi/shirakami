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

function(install_custom target_name export_name)
    install(
        TARGETS
            ${target_name}
        EXPORT
            ${export_name}
        LIBRARY
            DESTINATION ${CMAKE_INSTALL_LIBDIR}
            COMPONENT Runtime
        ARCHIVE
            DESTINATION ${CMAKE_INSTALL_LIBDIR}/${export_name}
            COMPONENT Development
        RUNTIME
            DESTINATION ${CMAKE_INSTALL_BINDIR}
            COMPONENT Runtime
    )
    # Install include files of interface libraries manually
    # INTERFACE_INCLUDE_DIRECTORIES must contains the following entries:
    # - one or more `$<BUILD_INTERFACE:...>` paths (may be absolute paths on source-tree)
    # - just one `$<INSTALL_INTERFACE:...>` path (must be a relative path from the install prefix)
    # then, this copies files in the BUILD_INTERFACE paths onto INSTALL_INTERFACE path
    # e.g.
    #   add_library(shakujo-interface INTERFACE)
    #   target_include_directories(shakujo-interface INTERFACE
    #       $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    #       $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/shakujo>)
    get_target_property(
        _includes
        ${target_name} INTERFACE_INCLUDE_DIRECTORIES
    )
    if (_includes)
        unset(_build_dir)
        unset(_install_dir)
        foreach (f IN LISTS _includes)
            if (f MATCHES "^\\$<BUILD_INTERFACE:(.+)>$")
                list(APPEND _build_dir ${CMAKE_MATCH_1})
            elseif (f MATCHES "^\\$<INSTALL_INTERFACE:(.+)>$")
                set(_install_dir ${CMAKE_MATCH_1})
            else()
                message(FATAL_ERROR "invalid include specification (${target_name}): ${f}")
            endif()
        endforeach()
        if (NOT _build_dir)
            message(FATAL_ERROR "${target_name} must declare \$<BUILD_INTERFACE:...> in INTERFACE_INCLUDE_DIRECTORIES")
        endif()
        if (NOT _install_dir)
            message(FATAL_ERROR "${target_name} must declare \$<INSTALL_INTERFACE:...> in INTERFACE_INCLUDE_DIRECTORIES")
        endif()
        install(
            DIRECTORY ${_build_dir}/
            DESTINATION ${_install_dir}
            COMPONENT Development
            PATTERN "doxygen.h" EXCLUDE
        )
    endif()
endfunction(install_custom)
