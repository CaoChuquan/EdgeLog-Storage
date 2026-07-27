cmake_minimum_required(VERSION 3.12)
project(EdgeLog-Storage)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)

find_package(Threads REQUIRED)

file(GLOB log_files CONFIGURE_DEPENDS ${CMAKE_SOURCE_DIR}/src/log/*.cpp)
file(GLOB config_files CONFIGURE_DEPENDS ${CMAKE_SOURCE_DIR}/src/config/*.cpp)

add_executable(edgelog-storage
    ${CMAKE_SOURCE_DIR}/src/main.cpp
    ${log_files}
    ${config_files}
)

target_include_directories(edgelog-storage PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/log
    ${CMAKE_SOURCE_DIR}/src/config
    ${CMAKE_SOURCE_DIR}/third_party
)

target_link_libraries(edgelog-storage PRIVATE
    Threads::Threads
)