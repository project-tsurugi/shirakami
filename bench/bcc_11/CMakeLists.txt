file(GLOB BCC_11_SOURCES
        "utility.cpp"
        "bcc_11.cpp"
        )

add_executable(bcc_11
        ${BCC_11_SOURCES}
        )

target_link_libraries(bcc_11
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
  target_link_libraries(bcc_11
        PRIVATE limestone
  )
endif()

target_include_directories(bcc_11
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
#        NAME    bcc_11
#        COMMAND bcc_11)
