file(GLOB SOURCES
        "*.cc"
)

add_library(kvs_charkey
    ${SOURCES}
)

target_include_directories(kvs_charkey
    PRIVATE include
)

find_package(Threads REQUIRED)

target_link_libraries(kvs_charkey
        PUBLIC api
        PRIVATE Threads::Threads
        Boost::filesystem
        ${PROJECT_SOURCE_DIR}/third_party/masstree-beta/libjson.a
)

set_target_properties(kvs_charkey
        PROPERTIES
        LIBRARY_OUTPUT_NAME "kvs"
        )

set_compile_options(kvs_charkey)
install_custom(kvs_charkey ${export_name})
clang_format(kvs_charkey)

# for tests
add_library(kvs_charkey-impl INTERFACE)

target_link_libraries(kvs_charkey-impl
)

target_include_directories(kvs_charkey-impl
    INTERFACE include
)
