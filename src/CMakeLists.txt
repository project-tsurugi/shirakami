file(GLOB SOURCES
        "kvs.cc"
        "xact.cc"
        "scheme.cc"
        "../third_party/masstree-beta/compiler.o"
        "../third_party/masstree-beta/kvthread.o"
        "../third_party/masstree-beta/misc.o"
        "../third_party/masstree-beta/clp.o"
        "../third_party/masstree-beta/libjson.a"
)

add_library(kvs_charkey
    ${SOURCES}
)

set_target_properties(kvs_charkey
        PROPERTIES
        LIBRARY_OUTPUT_NAME "kvs"
        )

target_include_directories(kvs_charkey
    PRIVATE .
)

target_link_libraries(kvs_charkey
        PUBLIC api
)

set_compile_options(kvs_charkey)
cmake_host_system_information(RESULT cores QUERY NUMBER_OF_LOGICAL_CORES)
add_definitions(-DKVS_NUMBER_OF_LOGICAL_CORES=${cores})

install_custom(kvs_charkey ${export_name})

# for tests
add_library(kvs_charkey-impl INTERFACE)

target_link_libraries(kvs_charkey-impl
)

target_include_directories(kvs_charkey-impl
    INTERFACE .
)

