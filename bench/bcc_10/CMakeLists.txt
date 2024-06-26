file(GLOB BCC_10_SOURCES
        "storage.cpp"
        "utility.cpp"
        "bcc_10.cpp"
        )

add_executable(bcc_10
        ${BCC_10_SOURCES}
        )

target_link_libraries(bcc_10
        PRIVATE gflags::gflags
        PRIVATE glog::glog
        PRIVATE shirakami
        PRIVATE shirakami-impl
        PRIVATE tbb
        PRIVATE tbbmalloc
        PRIVATE tbbmalloc_proxy
        PRIVATE Boost::filesystem
        PRIVATE Threads::Threads
        PRIVATE yakushima
        )

if (BUILD_PWAL)
  target_link_libraries(bcc_10
        PRIVATE limestone
  )
endif()

target_include_directories(bcc_10
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
#        NAME    bcc_10
#        COMMAND bcc_10)
