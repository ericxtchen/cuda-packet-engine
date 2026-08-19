cmake_minimum_required(VERSION 3.24)

set(CMAKE_CXX_COMPILER clang++-22)
set(CMAKE_CUDA_COMPILER clang++-22)
set(CUDAToolkit_ROOT "/usr/local/cuda")

project(cuda-packet-engine LANGUAGES CXX CUDA)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CUDA_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CUDA_ARCHITECTURES 89)

add_library(cpe
    src/generator/generator.cpp
    src/device/ingest_kernel.cu
    src/memory/memory.cu
)
target_include_directories(cpe PUBLIC include)
 
add_executable(cpe-bench apps/cpe-bench/main.cu)
target_link_libraries(cpe-bench PRIVATE cpe)
