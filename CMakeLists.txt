cmake_minimum_required(VERSION 3.10)
project(MiniDB)

set(CMAKE_CXX_STANDARD 17)

# 包含头文件目录
include_directories(include)

# 收集所有源文件
file(GLOB SOURCES
    src/cli/*.cpp
    src/parser/*.cpp
    src/storage/*.cpp
    src/executor/*.cpp
    src/db/*.cpp
    main.cpp
)

# 生成可执行文件
add_executable(mini_db ${SOURCES})