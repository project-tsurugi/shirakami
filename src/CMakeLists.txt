file(GLOB SOURCES
        "*.cpp"
        )

if (BUILD_CC_SILO_VARIANT)
    file(GLOB CC_SOURCES "concurrency_control/silo_variant/*.cpp")
    set(SOURCES ${SOURCES} ${CC_SOURCES})
endif ()

if (BUILD_INDEX_KOHLER_MASSTREE)
    file(GLOB INDEX_SOURCES "index/masstree_beta/*.cpp")
    set(SOURCES ${SOURCES} ${INDEX_SOURCES} index/yakushima/include/scheme.h)
endif ()

if (BUILD_PWAL)
    file(GLOB LOGGING_SOURCES "fault_tolerance/pwal.cpp")
    set(SOURCES ${SOURCES} ${LOGGING_SOURCES})
endif ()

if (BUILD_CPR)
    # TODO : fixme
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

if (BUILD_INDEX_KOHLER_MASSTREE)
    target_link_libraries(shirakami
            PUBLIC api
            PRIVATE Threads::Threads
            Boost::filesystem
            ${PROJECT_SOURCE_DIR}/third_party/masstree-beta/libjson.a
            )
elseif (BUILD_INDEX_YAKUSHIMA)
    target_link_libraries(shirakami
            PUBLIC api
            PRIVATE Threads::Threads
            Boost::filesystem
            )
endif ()

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
