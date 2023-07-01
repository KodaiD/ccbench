include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CompileOptions)

enable_testing()
add_subdirectory(test)

file(GLOB SILO_SOURCES
        "../common/util.cc"
        "epoch.cpp"
        "garbage_collection.cpp"
        "interface/interface_delete.cpp"
        "interface/interface_helper.cpp"
        "interface/interface_scan.cpp"
        "interface/interface_search.cpp"
        "interface/interface_termination.cpp"
        "interface/interface_update_insert.cpp"
        "log.cpp"
        "scheme.cpp"
        "session_info.cpp"
        "session_info_table.cpp"
        "tid.cpp"
        "index/masstree_beta/masstree_beta_wrapper.cpp"
        )

add_executable(silo.exe ${SILO_SOURCES} silo.cpp result.cpp util.cpp tpcc/tpcc_query.cpp tpcc/neworder.cpp tpcc/payment.cpp ../common/util.cc ../common/result.cc)

set_compile_options(silo.exe)

target_include_directories(silo.exe
        PRIVATE ${PROJECT_SOURCE_DIR}
        PRIVATE ${THIRD_PARTY_DIR}
        PRIVATE ${PROJECT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/index/masstree_beta/include
        PRIVATE ${PROJECT_SOURCE_DIR}/interface
        PRIVATE ${PROJECT_SOURCE_DIR}/tpcc
        )

target_link_libraries(silo.exe
        Boost::filesystem
        gflags::gflags
        ${THIRD_PARTY_DIR}/mimalloc/out/release/libmimalloc.a
        ${THIRD_PARTY_DIR}/masstree/libkohler_masstree_json.a
        Threads::Threads
        )

if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    add_definitions(-DCCBENCH_LINUX)
endif ()

cmake_host_system_information(RESULT cores QUERY NUMBER_OF_LOGICAL_CORES)
add_definitions(-DKVS_EPOCH_TIME=40)
add_definitions(-DKVS_NUMBER_OF_LOGICAL_CORES=${cores})
add_definitions(-DKVS_MAX_PARALLEL_THREADS=500)
add_definitions(-DKVS_MAX_KEY_LENGTH=1000)
add_definitions(-DKVS_LOG_GC_THRESHOLD=1)
add_definitions(-DPROJECT_ROOT=${PROJECT_SOURCE_DIR})

