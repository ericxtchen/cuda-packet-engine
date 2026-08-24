#include "cpe/spsc_queue.cuh"
#include <cstring>
#include <cuda/atomic>

SPSC_Queue::SPSC_Queue(const size_t count) {
  this->count = count;
  unsigned int flags = cudaHostAllocMapped | cudaHostAllocWriteCombined;
  cudaHostAlloc(reinterpret_cast<void **>(&this->host_ptr),
                count * sizeof(PacketFrame), flags);
  cudaHostGetDevicePointer(reinterpret_cast<void **>(&this->device_ptr),
                           this->host_ptr, 0);
}

SPSC_Queue::~SPSC_Queue() {
  cudaFreeHost(this->host_ptr);
  this->host_ptr = nullptr;
  this->device_ptr = nullptr;
}

SPSC_QueueArray alloc_spsc_queues(size_t num_queues, size_t capacity) {
  SPSC_QueueArray result;

  cudaHostAlloc(reinterpret_cast<void **>(&result.host),
                num_queues * sizeof(SPSC_Queue), cudaHostAllocMapped);
  cudaHostGetDevicePointer(reinterpret_cast<void **>(&result.device),
                           result.host, 0);

  for (size_t i = 0; i < num_queues; ++i) {
    new (&result.host[i]) SPSC_Queue(capacity);
  }

  return result;
}

void free_spsc_queues(SPSC_QueueArray &queues, size_t num_queues) {
  for (size_t i = 0; i < num_queues; ++i)
    queues.host[i].~SPSC_Queue();
  cudaFreeHost(queues.host);
  queues.host = nullptr;
  queues.device = nullptr;
}

bool push(SPSC_Queue *queue, const PacketFrame &frame) {
  cuda::atomic_ref<uint64_t, cuda::thread_scope_system> tail_ref(queue->tail);
  uint64_t tail = tail_ref.load(cuda::memory_order_acquire);

  if (queue->local_head - tail >= queue->count) {
    return false;
  }

  uint64_t index = queue->local_head % queue->count;
  queue->host_ptr[index] = frame;

  cuda::atomic_ref<uint64_t, cuda::thread_scope_system> head_ref(queue->head);
  head_ref.store(queue->local_head + 1, cuda::memory_order_release);

  (queue->local_head)++;
  return true;
}

__device__ void process(const PacketFrame &item, SPSC_Counters *counters) {
  (void)item;
  cuda::atomic_ref<uint64_t, cuda::thread_scope_system> consumed_ref(
      counters->consumed);
  consumed_ref.fetch_add(1ULL, cuda::memory_order_relaxed);
}

__global__ void pop(SPSC_Queue *queues, size_t num_queues,
                    const volatile int *running, SPSC_Counters *counters) {
  size_t q = blockIdx.x;
  if (q >= num_queues || threadIdx.x != 0)
    return;

  SPSC_Queue *queue = &queues[q];

  cuda::atomic_ref<uint64_t, cuda::thread_scope_system> head_ref(queue->head);
  cuda::atomic_ref<uint64_t, cuda::thread_scope_system> tail_ref(queue->tail);
  cuda::atomic_ref<uint64_t, cuda::thread_scope_system> errors_ref(
      counters->errors);

  uint64_t local_tail = 0;

  while (*running) {
    uint64_t head = head_ref.load(cuda::memory_order_acquire);

    if (local_tail == head) {
      continue;
    }

    uint64_t index = local_tail % queue->count;

    PacketFrame item = queue->device_ptr[index];

    uint64_t seq = 0;
    memcpy(&seq, item.payload, sizeof(seq));
    if (seq != local_tail) {
      errors_ref.fetch_add(1ULL, cuda::memory_order_relaxed);
    }

    process(item, counters);

    local_tail++;

    tail_ref.store(local_tail, cuda::memory_order_release);
  }
}