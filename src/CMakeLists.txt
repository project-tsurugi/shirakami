file(GLOB SOURCES
        "kvs.cc"
        "xact.cc"
        "lib.cc"
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
install_custom(kvs_charkey ${export_name})

# for tests
add_library(kvs_charkey-impl INTERFACE)

target_link_libraries(kvs_charkey-impl
)

target_include_directories(kvs_charkey-impl
    INTERFACE .
)

