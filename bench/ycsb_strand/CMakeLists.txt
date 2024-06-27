file(GLOB YCSB_STRAND_SOURCES
        "ycsb_strand.cpp"
        "build_db.cpp"
        "result.cpp"
        )

add_executable(ycsb_strand
        ${YCSB_STRAND_SOURCES}
        )

target_link_libraries(ycsb_strand
        PRIVATE gflags::gflags
        PRIVATE glog::glog
        PRIVATE shirakami
        PRIVATE shirakami-impl
        PRIVATE ${tbb_prefix}tbb
        PRIVATE ${tbb_prefix}tbbmalloc
        PRIVATE ${tbb_prefix}tbbmalloc_proxy
        PRIVATE Boost::filesystem
        PRIVATE Threads::Threads
        PRIVATE yakushima
        PRIVATE atomic
        )

if (BUILD_PWAL)
  target_link_libraries(ycsb_strand
        PRIVATE limestone
  )
endif()

target_include_directories(ycsb_strand
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/bench
        PRIVATE ${PROJECT_SOURCE_DIR}/bench/include
        PRIVATE ${PROJECT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/src
        PRIVATE ${PROJECT_SOURCE_DIR}/src/include
        PRIVATE ${PROJECT_SOURCE_DIR}/test/include
        PRIVATE ${gflags_INCLUDE_DIR}
        )

add_test(
        NAME ycsb_strand
        COMMAND ycsb_strand)