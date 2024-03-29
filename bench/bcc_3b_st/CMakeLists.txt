file(GLOB BCC_3B_ST_SOURCES
        "storage.cpp"
        "utility.cpp"
        "bcc_3b_st.cpp"
        )

add_executable(bcc_3b_st
        ${BCC_3B_ST_SOURCES}
        )

target_link_libraries(bcc_3b_st
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

target_include_directories(bcc_3b_st
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/bench/include
        PRIVATE ${PROJECT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/src
        PRIVATE ${PROJECT_SOURCE_DIR}/src/include
        PRIVATE ${PROJECT_SOURCE_DIR}/test/include
        PRIVATE ${gflags_INCLUDE_DIR}
        )

if (NOT BUILD_WP)
add_test(
        NAME    bcc_3b_st
        COMMAND bcc_3b_st)
endif()
