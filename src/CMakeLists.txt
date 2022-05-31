file(GLOB SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/fault_tolerance/log.cpp"
        )

# about cc
file(GLOB CC_SOURCES 
"concurrency_control/*.cpp"
"concurrency_control/wp/*.cpp"
"concurrency_control/wp/interface/*.cpp"
"concurrency_control/wp/interface/long_tx/*.cpp"
"concurrency_control/wp/interface/short_tx/*.cpp"
"datastore/limestone/*.cpp"
)
set(SOURCES ${SOURCES} ${CC_SOURCES})

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
        PRIVATE limestone
        PRIVATE Boost::filesystem
        PRIVATE glog::glog
        PRIVATE tsl::hopscotch_map
        PRIVATE ${tbb_prefix}tbb
        PRIVATE ${tbb_prefix}tbbmalloc
        PRIVATE ${tbb_prefix}tbbmalloc_proxy
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
