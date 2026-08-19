#include "cpe/ingest_kernel.cuh"
#include "cpe/packet_envelope.hpp"

__global__ void count_packets(const uint8_t *d_buf, size_t num_packets,
                              unsigned long long *d_count) {
  size_t i = blockIdx.x * blockDim.x + threadIdx.x;

  if (i < num_packets) {
    size_t byte_offset = i * SLOT_SIZE;
    if (d_buf[byte_offset] != 0xFF) {
      atomicAdd(d_count, 1ULL);
    }
  }
}