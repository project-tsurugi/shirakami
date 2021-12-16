file(GLOB BCC_4_SOURCES
        "storage.cpp"
        "utility.cpp"
        "bcc_4.cpp"
        )

add_executable(bcc_4
        ${BCC_4_SOURCES}
        )

target_link_libraries(bcc_4
        PRIVATE gflags::gflags
        PRIVATE glog::glog
        PRIVATE shirakami
        PRIVATE shirakami-impl
        PRIVATE tsl::hopscotch_map
        PRIVATE tbb
        PRIVATE tbbmalloc
        PRIVATE tbbmalloc_proxy
        PRIVATE Boost::filesystem
        PRIVATE Threads::Threads
        )

target_include_directories(bcc_4
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/bench/include
        PRIVATE ${PROJECT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/src
        PRIVATE ${PROJECT_SOURCE_DIR}/src/include
        PRIVATE ${PROJECT_SOURCE_DIR}/test/include
        PRIVATE ${PROJECT_SOURCE_DIR}/third_party
        PRIVATE ${PROJECT_SOURCE_DIR}/third_party/msgpack-c/include
        PRIVATE ${gflags_INCLUDE_DIR}
        )

#add_test(
#        NAME    bcc_4
#        COMMAND bcc_4)