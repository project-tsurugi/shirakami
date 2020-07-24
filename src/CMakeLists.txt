file(GLOB SOURCES
        "*.cpp"
        "cc/*.cpp"
        "index/index.cpp"
        )

if (BUILD_INDEX_KOHLER_MASSTREE)
    file(GLOB INDEX_SOURCES "index/masstree_beta_wrapper.cpp")
    set(SOURCES ${SOURCES} ${INDEX_SOURCES})
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
        PRIVATE Threads::Threads
        Boost::filesystem
        ${PROJECT_SOURCE_DIR}/third_party/masstree-beta/libjson.a
        )

set_target_properties(shirakami
        PROPERTIES
        LIBRARY_OUTPUT_NAME "kvs"
        )

set_compile_options(shirakami)
install_custom(shirakami ${export_name})
clang_format(shirakami)

# for tests
add_library(shirakami-impl INTERFACE)

target_link_libraries(shirakami-impl
        )

target_include_directories(shirakami-impl
        INTERFACE include
        )
