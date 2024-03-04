file(GLOB SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/fault_tolerance/log.cpp"
        )

# about cc
file(GLOB CC_SOURCES
"concurrency_control/bg_work/*.cpp"
"concurrency_control/*.cpp"
"concurrency_control/interface/*.cpp"
"concurrency_control/interface/long_tx/*.cpp"
"concurrency_control/interface/read_only_tx/*.cpp"
"concurrency_control/interface/short_tx/*.cpp"
)
set(SOURCES ${SOURCES} ${CC_SOURCES})

# about logging
if (BUILD_PWAL)
  file(GLOB LOGGING_SOURCES
    "datastore/limestone/*.cpp"
  )
  set(SOURCES ${SOURCES} ${LOGGING_SOURCES})
endif()

# about database
file (GLOB DATABASE_SOURCES
"database/*.cpp"
)
set(SOURCES ${SOURCES} ${DATABASE_SOURCES})

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
        PRIVATE Boost::thread
        PRIVATE glog::glog
        PRIVATE ${tbb_prefix}tbb
        PRIVATE ${tbb_prefix}tbbmalloc
        PRIVATE ${tbb_prefix}tbbmalloc_proxy
        PRIVATE Threads::Threads
        PRIVATE yakushima
        )

if (BUILD_PWAL)
  target_link_libraries(shirakami
        PRIVATE limestone
  )
endif()

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
