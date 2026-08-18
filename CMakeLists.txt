cmake_minimum_required(VERSION 3.12)
project(EdgeLog-Storage)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)

find_package(Threads REQUIRED)

# 定位 MySQL C 客户端开发库（libmysqlclient-dev 提供 mysql.h 与 libmysqlclient）
find_path(MYSQL_INCLUDE_DIR mysql/mysql.h
    PATHS /usr/include /usr/local/include /usr/include/mysql)
find_library(MYSQL_LIBRARY NAMES mysqlclient mariadb
    PATHS /usr/lib /usr/lib/x86_64-linux-gnu /usr/local/lib /usr/lib/mysql)
if(NOT MYSQL_INCLUDE_DIR OR NOT MYSQL_LIBRARY)
    message(FATAL_ERROR
        "MySQL client dev library not found. Install it, e.g.:\n"
        "  sudo apt install libmysqlclient-dev")
endif()

file(GLOB log_files CONFIGURE_DEPENDS ${CMAKE_SOURCE_DIR}/src/log/*.cpp)
file(GLOB config_files CONFIGURE_DEPENDS ${CMAKE_SOURCE_DIR}/src/config/*.cpp)
file(GLOB net_files CONFIGURE_DEPENDS ${CMAKE_SOURCE_DIR}/src/net/*.cpp)
file(GLOB threadpool_files CONFIGURE_DEPENDS ${CMAKE_SOURCE_DIR}/src/threadpool/*.cpp)
file(GLOB storage_files CONFIGURE_DEPENDS ${CMAKE_SOURCE_DIR}/src/storage/*.cpp)
file(GLOB device_files CONFIGURE_DEPENDS ${CMAKE_SOURCE_DIR}/src/device/*.cpp)

# 核心模块打包成静态库，供主程序与测试共同链接，避免源文件列表重复
add_library(edgelog_core STATIC
    ${log_files}
    ${config_files}
    ${net_files}
    ${threadpool_files}
    ${storage_files}
    ${device_files}
)

target_include_directories(edgelog_core PUBLIC
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/log
    ${CMAKE_SOURCE_DIR}/src/config
    ${CMAKE_SOURCE_DIR}/src/net
    ${CMAKE_SOURCE_DIR}/src/threadpool
    ${CMAKE_SOURCE_DIR}/src/storage
    ${CMAKE_SOURCE_DIR}/src/device
    ${CMAKE_SOURCE_DIR}/third_party
    ${MYSQL_INCLUDE_DIR}
)

target_link_libraries(edgelog_core PUBLIC Threads::Threads ${MYSQL_LIBRARY})

add_executable(edgelog-storage
    ${CMAKE_SOURCE_DIR}/src/main.cpp
)

target_link_libraries(edgelog-storage PRIVATE edgelog_core)

# 测试
enable_testing()
add_executable(test_network ${CMAKE_SOURCE_DIR}/tests/test_network.cpp)
target_link_libraries(test_network PRIVATE edgelog_core)
add_test(NAME network_e2e COMMAND test_network)

add_executable(test_storage ${CMAKE_SOURCE_DIR}/tests/test_storage.cpp)
target_link_libraries(test_storage PRIVATE edgelog_core)
target_compile_definitions(test_storage PRIVATE
    EDGELOG_CONFIG_PATH="${CMAKE_SOURCE_DIR}/config.json")
add_test(NAME storage_e2e COMMAND test_storage)

add_executable(test_storage_bench ${CMAKE_SOURCE_DIR}/tests/test_storage_bench.cpp)
target_link_libraries(test_storage_bench PRIVATE edgelog_core)
target_compile_definitions(test_storage_bench PRIVATE
    EDGELOG_CONFIG_PATH="${CMAKE_SOURCE_DIR}/config.json")
add_test(NAME storage_bench COMMAND test_storage_bench)

# 弱网可靠性端到端测试（设备端 WAL + ACK + 断线重传 + 崩溃恢复），不依赖 MySQL
add_executable(test_reliable ${CMAKE_SOURCE_DIR}/tests/test_reliable.cpp)
target_link_libraries(test_reliable PRIVATE edgelog_core)
add_test(NAME reliable_e2e COMMAND test_reliable)