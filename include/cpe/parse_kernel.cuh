#ifndef PARSE_KERNEL_H
#define PARSE_KERNEL_H

#include "packet_envelope.hpp"
#include "spsc_queue.cuh"
#include <cstdint>
#include <cuda_runtime.h>

// Number of flow buckets for the 5-tuple classifier. Kept small and a power
// of two so `hash % NUM_FLOW_BUCKETS` is cheap and the histogram fits
// comfortably in one cache line * N for host-side inspection.
constexpr uint32_t NUM_FLOW_BUCKETS = 256;

struct ParseCounters {
  uint64_t consumed{0};
  uint64_t errors{0};
  uint64_t checksum_failures{0};
  uint64_t flow_hist[NUM_FLOW_BUCKETS]{};
};

__device__ bool device_ipv4_checksum_valid(const IPv4Header &hdr);

__device__ uint32_t hash_five_tuple(uint32_t src_ip, uint32_t dst_ip,
                                    uint16_t src_port, uint16_t dst_port,
                                    uint8_t protocol);

// Each iteration, the warp claims up to 32 contiguous outstanding packets from
// the ring (seq_index = local_tail + lane) and each lane parses its own packet
// in parallel with checksum validation, 5-tuple hash, and flow bucket instead
// of one thread doing all of that serially. Launch with blockDim.x == 32,
// gridDim.x == num_queues.
__global__ void pop_and_parse(SPSC_Queue *queues, size_t num_queues,
                              const volatile int *running,
                              ParseCounters *counters);

#endif
