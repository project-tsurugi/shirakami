file(GLOB ROCKSDB_SOURCES
        "rocksdb_bench.cpp"
        )

if (BUILD_ROCKSDB_BENCH)
    add_executable(rocksdb_bench
            ${ROCKSDB_SOURCES}
            )

    target_link_libraries(rocksdb_bench
            PRIVATE glog::glog
            PRIVATE gflags::gflags
            PRIVATE Threads::Threads
            PRIVATE librocksdb.so
            )

    target_include_directories(rocksdb_bench
            PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
            PRIVATE ${PROJECT_SOURCE_DIR}/src/include
            PRIVATE ${gflags_INCLUDE_DIR}
            )
endif ()
