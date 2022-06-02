file(GLOB BCC_7_SOURCES
        "storage.cpp"
        "utility.cpp"
        "bcc_7.cpp"
        )

add_executable(bcc_7
        ${BCC_7_SOURCES}
        )

target_link_libraries(bcc_7
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

if (BUILD_PWAL)
  target_link_libraries(bcc_7
        PRIVATE limestone
  )
endif()

target_include_directories(bcc_7
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

# Debug builds take a long time and cannot be automated. Release builds finish 
# in a realistic amount of time.
#add_test(
#        NAME    bcc_7
#        COMMAND bcc_7)