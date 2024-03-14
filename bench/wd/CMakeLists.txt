file(GLOB TI_SOURCES
        "tui686.cpp"
        )

add_executable(tui686
        ${TI_SOURCES}
        )

target_link_libraries(tui686
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
  target_link_libraries(tui686
        PRIVATE limestone
  )
endif()

target_include_directories(tui686
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
        NAME tui686
        COMMAND tui686)