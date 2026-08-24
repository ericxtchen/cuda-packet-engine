#include "cpe/parse_kernel.cuh"
#include <cstring>
#include <cuda/atomic>

__device__ bool device_ipv4_checksum_valid(const IPv4Header &hdr) {
  const uint16_t *data = reinterpret_cast<const uint16_t *>(&hdr);
  uint32_t acc = 0;

#pragma unroll
  for (int i = 0; i < static_cast<int>(sizeof(IPv4Header) / 2); ++i) {
    acc += data[i];
  }

  while (acc >> 16) {
    acc = (acc & 0xFFFF) + (acc >> 16);
  }
  return static_cast<uint16_t>(~acc) == 0;
}

__device__ uint32_t hash_five_tuple(uint32_t src_ip, uint32_t dst_ip,
                                    uint16_t src_port, uint16_t dst_port,
                                    uint8_t protocol) {
  uint32_t h = 2166136261u; // FNV-1a offset basis

  h ^= src_ip;
  h *= 16777619u;
  h ^= dst_ip;
  h *= 16777619u;
  h ^= (static_cast<uint32_t>(src_port) << 16) | dst_port;
  h *= 16777619u;
  h ^= protocol;
  h *= 16777619u;

  return h;
}

namespace {

__device__ void parse_one(const PacketFrame &pkt, ParseCounters *counters) {
  if (!device_ipv4_checksum_valid(pkt.ip)) {
    cuda::atomic_ref<uint64_t, cuda::thread_scope_system> cs_ref(
        counters->checksum_failures);
    cs_ref.fetch_add(1ULL, cuda::memory_order_relaxed);
  }

  uint32_t h = hash_five_tuple(pkt.ip.src_ip, pkt.ip.dest_ip, pkt.udp.src_port,
                               pkt.udp.dest_port, pkt.ip.protocol);
  uint32_t bucket = h % NUM_FLOW_BUCKETS;

  cuda::atomic_ref<uint64_t, cuda::thread_scope_system> bucket_ref(
      counters->flow_hist[bucket]);
  bucket_ref.fetch_add(1ULL, cuda::memory_order_relaxed);

  cuda::atomic_ref<uint64_t, cuda::thread_scope_system> consumed_ref(
      counters->consumed);
  consumed_ref.fetch_add(1ULL, cuda::memory_order_relaxed);
}

} // namespace

__global__ void pop_and_parse(SPSC_Queue *queues, size_t num_queues,
                              const volatile int *running,
                              ParseCounters *counters) {
  size_t q = blockIdx.x;
  if (q >= num_queues)
    return;

  const int lane = threadIdx.x; // 0..31, one warp per block/queue
  SPSC_Queue *queue = &queues[q];

  cuda::atomic_ref<uint64_t, cuda::thread_scope_system> head_ref(queue->head);
  cuda::atomic_ref<uint64_t, cuda::thread_scope_system> tail_ref(queue->tail);
  cuda::atomic_ref<uint64_t, cuda::thread_scope_system> errors_ref(
      counters->errors);

  uint64_t local_tail = 0;

  while (*running) {
    uint64_t head = head_ref.load(cuda::memory_order_acquire);
    uint64_t available = head - local_tail;

    if (available == 0) {
      continue;
    }

    // Claim a contiguous, warp-sized batch instead of one packet.
    uint64_t batch = available < static_cast<uint64_t>(warpSize)
                         ? available
                         : static_cast<uint64_t>(warpSize);

    if (static_cast<uint64_t>(lane) < batch) {
      uint64_t seq_index = local_tail + static_cast<uint64_t>(lane);
      uint64_t slot = seq_index % queue->count;

      PacketFrame item = queue->device_ptr[slot];

      uint64_t seq = 0;
      memcpy(&seq, item.payload, sizeof(seq));
      if (seq != seq_index) {
        errors_ref.fetch_add(1ULL, cuda::memory_order_relaxed);
      }

      parse_one(item, counters);
    }

    // Every lane must have finished reading its slot before we advance
    // local_tail and publish it. Otherwise the producer could see the
    // freed space and overwrite a slot a slow lane hasn't read yet.
    __syncwarp();

    local_tail += batch;
    if (lane == 0) {
      tail_ref.store(local_tail, cuda::memory_order_release);
    }
    __syncwarp();
  }
}
