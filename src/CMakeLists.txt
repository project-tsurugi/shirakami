file(GLOB SOURCES
        "*.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/fault_tolerance/log.cpp"
        )

# about cc
file(GLOB CC_SOURCES "concurrency_control/silo_variant/*.cpp")
set(SOURCES ${SOURCES} ${CC_SOURCES})

if (BUILD_PWAL)
    file(GLOB LOGGING_SOURCES "fault_tolerance/pwal.cpp")
    set(SOURCES ${SOURCES} ${LOGGING_SOURCES})
endif ()

if (BUILD_CPR)
    file(GLOB LOGGING_SOURCES "fault_tolerance/cpr.cpp")
    set(SOURCES ${SOURCES} ${LOGGING_SOURCES})
endif ()

add_library(shirakami
        ${SOURCES}
        )

target_include_directories(shirakami
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/fault_tolerance/include
        PRIVATE ${PROJECT_SOURCE_DIR}/third_party
        PRIVATE ${PROJECT_SOURCE_DIR}/third_party/msgpack-c/include
        )

find_package(Threads REQUIRED)

target_link_libraries(shirakami
        PUBLIC api
        PRIVATE Boost::filesystem
        PRIVATE tsl::hopscotch_map
        PRIVATE tbb
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
