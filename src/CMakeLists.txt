file(GLOB SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/fault_tolerance/log.cpp"
        )

# about cc
if (BUILD_WP)
    file(GLOB CC_SOURCES 
    "concurrency_control/wp/*.cpp"
    "concurrency_control/wp/interface/*.cpp"
    )
    set(SOURCES ${SOURCES} ${CC_SOURCES})
else ()
    file(GLOB CC_SOURCES 
    "concurrency_control/silo/*.cpp"
    "concurrency_control/silo/interface/*.cpp"
    )
    set(SOURCES ${SOURCES} ${CC_SOURCES})

    if (BUILD_PWAL)
        file(GLOB LOGGING_SOURCES "fault_tolerance/pwal.cpp")
        set(SOURCES ${SOURCES} ${LOGGING_SOURCES})
    endif ()
    
    if (BUILD_CPR)
        file(GLOB LOGGING_SOURCES "fault_tolerance/cpr.cpp")
        set(SOURCES ${SOURCES} ${LOGGING_SOURCES})
    endif ()
endif ()

add_library(shirakami
        ${SOURCES}
        )

target_include_directories(shirakami
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/third_party
        )

find_package(Threads REQUIRED)

target_link_libraries(shirakami
        PUBLIC api
        PRIVATE Boost::filesystem
        PRIVATE glog::glog
        PRIVATE tsl::hopscotch_map
        PRIVATE tbb
        PRIVATE tbbmalloc
        PRIVATE tbbmalloc_proxy
        PRIVATE Threads::Threads
        )

set_target_properties(shirakami
        PROPERTIES
        LIBRARY_OUTPUT_NAME "kvs"
        )

set_compile_options(shirakami)
install_custom(shirakami ${export_name})

if (FORMAT_FILES_WITH_CLANG_FORMAT_BEFORE_EACH_BUILD)
    clang_format(shirakami)
endif ()

# for tests
add_library(shirakami-impl INTERFACE)

target_link_libraries(shirakami-impl
        )

target_include_directories(shirakami-impl
        INTERFACE include
        )
