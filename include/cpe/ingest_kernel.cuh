#ifndef INGEST_KERNEL_H
#define INGEST_KERNEL_H

#include <cstdint>
#include <cuda_runtime.h>

__global__ void count_packets(const uint8_t *d_buf, size_t num_packets,
                              unsigned long long *d_count);

#endif