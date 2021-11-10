file(GLOB MAP_SOURCES
        "map.cpp"
        )

add_executable(map
        ${MAP_SOURCES}
        )

target_include_directories(map
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/bench/include
        PRIVATE ${PROJECT_SOURCE_DIR}/src/include
        PRIVATE ${gflags_INCLUDE_DIR}
        )

target_link_libraries(map
        PRIVATE glog::glog
        PRIVATE gflags::gflags
        PRIVATE tsl::hopscotch_map
        PRIVATE Threads::Threads
        )
