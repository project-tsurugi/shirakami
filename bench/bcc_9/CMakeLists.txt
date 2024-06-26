file(GLOB BCC_9_SOURCES
        "storage.cpp"
        "utility.cpp"
        "bcc_9.cpp"
        )

add_executable(bcc_9
        ${BCC_9_SOURCES}
        )

target_link_libraries(bcc_9
        PRIVATE gflags::gflags
        PRIVATE glog::glog
        PRIVATE shirakami
        PRIVATE shirakami-impl
        PRIVATE tbb
        PRIVATE tbbmalloc
        PRIVATE tbbmalloc_proxy
        PRIVATE Boost::filesystem
        PRIVATE Threads::Threads
        )

target_include_directories(bcc_9
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/bench/include
        PRIVATE ${PROJECT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/src
        PRIVATE ${PROJECT_SOURCE_DIR}/src/include
        PRIVATE ${PROJECT_SOURCE_DIR}/test/include
        PRIVATE ${gflags_INCLUDE_DIR}
        )

# Debug builds take a long time and cannot be automated. Release builds finish
# in a realistic amount of time.
#add_test(
#        NAME    bcc_9
#        COMMAND bcc_9)
