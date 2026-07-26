cmake_minimum_required(VERSION 3.10)
project(EdgeLog-Storage)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

file(GLOB log_sources src/log/*.cpp)
file(GLOB config_sources src/config/*.cpp)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_SOURCE_DIR}/bin)

add_executable(EdgeLog-Storage 
    ${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp
    ${log_sources}
    ${config_sources}
)

target_include_directories(EdgeLog-Storage PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_include_directories(EdgeLog-Storage PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
target_link_libraries(EdgeLog-Storage PRIVATE pthread stdc++fs)
target_link_libraries(EdgeLog-Storage PRIVATE nlohmann_json::nlohmann_json)