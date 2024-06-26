file(GLOB YCSB_MB_NC_SOURCES
        "ycsb_mb_nc.cpp"
        "build_db.cpp"
        "result.cpp"
        )

add_executable(ycsb_mb_nc
        ${YCSB_MB_NC_SOURCES}
        )

target_link_libraries(ycsb_mb_nc
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
        )

if (BUILD_PWAL)
  target_link_libraries(ycsb_mb_nc
        PRIVATE limestone
  )
endif()

target_include_directories(ycsb_mb_nc
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/bench/include
        PRIVATE ${PROJECT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/src
        PRIVATE ${PROJECT_SOURCE_DIR}/src/include
        PRIVATE ${PROJECT_SOURCE_DIR}/test/include
        PRIVATE ${gflags_INCLUDE_DIR}
        )

add_test(
        NAME ycsb_mb_nc
        COMMAND ycsb_mb_nc)
