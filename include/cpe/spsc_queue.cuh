#ifndef SPSC_QUEUE_H
#define SPSC_QUEUE_H

#include "packet_envelope.hpp"
#include <cstddef>
#include <cstdint>

struct alignas(64) SPSC_Queue {
  PacketFrame *host_ptr = nullptr;
  PacketFrame *device_ptr = nullptr;
  size_t count = 0;

  uint64_t local_head = 0;

  alignas(64) uint64_t head{0};
  alignas(64) uint64_t tail{0};

  SPSC_Queue(const size_t count);
  ~SPSC_Queue();
};

struct SPSC_QueueArray {
  SPSC_Queue *host = nullptr;
  SPSC_Queue *device = nullptr;
};

struct SPSC_Counters {
  uint64_t consumed{0};
  uint64_t errors{0};
};

SPSC_QueueArray alloc_spsc_queues(size_t num_queues, size_t capacity);
void free_spsc_queues(SPSC_QueueArray &queues, size_t num_queues);

bool push(SPSC_Queue *queue, const PacketFrame &frame);

__global__ void pop(SPSC_Queue *queues, size_t num_queues,
                    const volatile int *running, SPSC_Counters *counters);

#endif