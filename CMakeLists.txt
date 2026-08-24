cmake_minimum_required(VERSION 3.24)

set(CUDAToolkit_ROOT "/usr/local/cuda")

project(cuda-packet-engine LANGUAGES CXX CUDA)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CUDA_STANDARD 20)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)

set(CMAKE_CUDA_ARCHITECTURES 75)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Find CUDA runtime and other toolkit libraries
find_package(CUDAToolkit REQUIRED)

set(CUDA_TARGET_INCLUDE_DIR
    "${CUDAToolkit_TARGET_DIR}/include"
)

set(CUDA_CCCL_INCLUDE_DIR
    "${CUDAToolkit_TARGET_DIR}/include/cccl"
)

add_library(cpe
    src/generator.cu
    src/ingest_kernel.cu
    src/memory.cu
    src/spsc_queue.cu
    src/affinity.cpp
    src/parse_kernel.cu
)

target_include_directories(cpe
    PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CUDA_TARGET_INCLUDE_DIR}"
        "${CUDA_CCCL_INCLUDE_DIR}"
)

# CUDA runtime
target_link_libraries(cpe
    PUBLIC
        CUDA::cudart
)

add_executable(cpe-bench
    apps/cpe-bench/main.cu
)

target_link_libraries(cpe-bench
    PRIVATE
        cpe
)

find_package(Threads REQUIRED)
target_link_libraries(cpe PRIVATE Threads::Threads)