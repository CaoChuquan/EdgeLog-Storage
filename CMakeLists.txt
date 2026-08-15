cmake_minimum_required(VERSION 3.12)
project(EdgeLog-Storage)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)

find_package(Threads REQUIRED)

file(GLOB log_files CONFIGURE_DEPENDS ${CMAKE_SOURCE_DIR}/src/log/*.cpp)
file(GLOB config_files CONFIGURE_DEPENDS ${CMAKE_SOURCE_DIR}/src/config/*.cpp)
file(GLOB net_files CONFIGURE_DEPENDS ${CMAKE_SOURCE_DIR}/src/net/*.cpp)
file(GLOB threadpool_files CONFIGURE_DEPENDS ${CMAKE_SOURCE_DIR}/src/threadpool/*.cpp)

# 核心模块打包成静态库，供主程序与测试共同链接，避免源文件列表重复
add_library(edgelog_core STATIC
    ${log_files}
    ${config_files}
    ${net_files}
    ${threadpool_files}
)

target_include_directories(edgelog_core PUBLIC
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/log
    ${CMAKE_SOURCE_DIR}/src/config
    ${CMAKE_SOURCE_DIR}/src/net
    ${CMAKE_SOURCE_DIR}/src/threadpool
    ${CMAKE_SOURCE_DIR}/third_party
)

target_link_libraries(edgelog_core PUBLIC Threads::Threads)

add_executable(edgelog-storage
    ${CMAKE_SOURCE_DIR}/src/main.cpp
)

target_link_libraries(edgelog-storage PRIVATE edgelog_core)

# 测试
enable_testing()
add_executable(test_network ${CMAKE_SOURCE_DIR}/tests/test_network.cpp)
target_link_libraries(test_network PRIVATE edgelog_core)
add_test(NAME network_e2e COMMAND test_network)