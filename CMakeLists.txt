cmake_minimum_required(VERSION 3.10)
project(MiniDB_AI)

add_compile_options(/utf-8)
set(CMAKE_CXX_STANDARD 17)

# 抑制特定的编译器警告
if(MSVC)
    add_compile_options(/wd4267 /wd4244)  # 抑制size_t转换警告
endif()

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