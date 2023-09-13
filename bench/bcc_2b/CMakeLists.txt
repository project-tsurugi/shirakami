file(GLOB BCC_2B_SOURCES
        "storage.cpp"
        "utility.cpp"
        "bcc_2b.cpp"
        )

add_executable(bcc_2b
        ${BCC_2B_SOURCES}
        )

target_link_libraries(bcc_2b
        PRIVATE gflags::gflags
        PRIVATE glog::glog
        PRIVATE shirakami
        PRIVATE shirakami-impl
        PRIVATE ${tbb_prefix}tbb
        PRIVATE ${tbb_prefix}tbbmalloc
        PRIVATE ${tbb_prefix}tbbmalloc_proxy
        PRIVATE Boost::filesystem
        PRIVATE Threads::Threads
        )

target_include_directories(bcc_2b
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/bench/include
        PRIVATE ${PROJECT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/src
        PRIVATE ${PROJECT_SOURCE_DIR}/src/include
        PRIVATE ${PROJECT_SOURCE_DIR}/test/include
        PRIVATE ${gflags_INCLUDE_DIR}
        )

add_test(
        NAME    bcc_2b
        COMMAND bcc_2b)