#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "cpe/generator.cuh"
#include "cpe/ingest_kernel.cuh"
#include "cpe/memory.hpp"
#include "cpe/packet_envelope.hpp"
#include "cpe/spsc_queue.cuh"

using clock_type = std::chrono::steady_clock;

struct PipelineConfig {
  const char *name;
  AllocMode alloc_mode;
  size_t num_queues = 1;
};

struct BenchResult {
  double batches_per_sec = 0;
  double packets_per_sec = 0;
  double gb_per_sec = 0;
  double alloc_time_ms = 0;
  unsigned long long validation_errors = 0;
};

namespace {

constexpr size_t SPSC_QUEUE_CAPACITY = 4096;

BenchResult run_spsc_pipeline(size_t num_queues, double warmup_seconds,
                              double timed_seconds) {
  BenchResult result;

  if (num_queues == 0)
    return result;

  SPSC_QueueArray queues = alloc_spsc_queues(num_queues, SPSC_QUEUE_CAPACITY);

  int *running = nullptr;
  cudaHostAlloc(reinterpret_cast<void **>(&running), sizeof(int),
                cudaHostAllocMapped);
  int *running_dev = nullptr;
  cudaHostGetDevicePointer(reinterpret_cast<void **>(&running_dev), running, 0);
  *running = 1;

  SPSC_Counters *counters = nullptr;
  cudaHostAlloc(reinterpret_cast<void **>(&counters), sizeof(SPSC_Counters),
                cudaHostAllocMapped);
  SPSC_Counters *counters_dev = nullptr;
  cudaHostGetDevicePointer(reinterpret_cast<void **>(&counters_dev), counters,
                           0);
  counters->consumed = 0;
  counters->errors = 0;

  std::vector<SPSC_Queue *> queue_ptrs(num_queues);
  for (size_t i = 0; i < num_queues; ++i)
    queue_ptrs[i] = &queues.host[i];

  std::atomic<bool> producers_running{true};
  auto threads = start_generators(queue_ptrs, &producers_running);

  size_t active_queues = queue_ptrs.size();
  if (active_queues == 0) {
    producers_running.store(false);
    *running = 0;
    cudaFreeHost(counters);
    cudaFreeHost(running);
    free_spsc_queues(queues, num_queues);
    return result;
  }

  cudaStream_t stream;
  cudaStreamCreate(&stream);

  pop<<<static_cast<int>(active_queues), 1, 0, stream>>>(
      queues.device, active_queues, running_dev, counters_dev);

  std::this_thread::sleep_for(std::chrono::duration<double>(warmup_seconds));

  // Non-blocking zero-copy reads directly on host
  uint64_t start_consumed = counters->consumed;
  uint64_t start_errors = counters->errors;

  auto t_start = clock_type::now();
  std::this_thread::sleep_for(std::chrono::duration<double>(timed_seconds));
  auto t_end = clock_type::now();

  uint64_t end_consumed = counters->consumed;
  uint64_t end_errors = counters->errors;

  producers_running.store(false, std::memory_order_relaxed);
  for (auto &t : threads)
    t.join();

  *running = 0;
  cudaStreamSynchronize(stream);
  cudaStreamDestroy(stream);

  uint64_t consumed =
      (end_consumed >= start_consumed) ? (end_consumed - start_consumed) : 0;
  uint64_t errors =
      (end_errors >= start_errors) ? (end_errors - start_errors) : 0;

  double elapsed_s = std::chrono::duration<double>(t_end - t_start).count();
  result.packets_per_sec =
      elapsed_s > 0 ? static_cast<double>(consumed) / elapsed_s : 0.0;
  result.gb_per_sec =
      elapsed_s > 0
          ? (static_cast<double>(consumed) * sizeof(PacketFrame) / elapsed_s) /
                1e9
          : 0.0;
  result.validation_errors = errors;

  free_spsc_queues(queues, num_queues);
  cudaFreeHost(counters);
  cudaFreeHost(running);

  return result;
}

BenchResult run_batch_pipeline(const PipelineConfig &cfg, int num_warmup,
                               int num_timed) {
  BenchResult result;

  auto t_alloc_start = clock_type::now();
  HostBuffer buf = allocate_host_buffer(cfg.alloc_mode, BATCH_BYTES);
  auto t_alloc_end = clock_type::now();

  unsigned long long h_count = 0;
  unsigned long long *d_count = nullptr;
  cudaMalloc((void **)&d_count, sizeof(unsigned long long));

  int threadsPerBlock = 256;
  int blocksPerGrid = (BATCH_PACKETS + threadsPerBlock - 1) / threadsPerBlock;

  auto run_batch = [&]() {
    generate_traffic(buf.host_ptr, BATCH_PACKETS);
    sync_to_device(buf, BATCH_BYTES);
    cudaMemset(d_count, 0, sizeof(unsigned long long));
    count_packets<<<blocksPerGrid, threadsPerBlock>>>(buf.device_ptr,
                                                      BATCH_PACKETS, d_count);
    cudaDeviceSynchronize();
  };

  for (int i = 0; i < num_warmup; ++i)
    run_batch();

  auto t_start = clock_type::now();
  for (int batch = 0; batch < num_timed; ++batch)
    run_batch();
  auto t_end = clock_type::now();

  double elapsed_s = std::chrono::duration<double>(t_end - t_start).count();
  double total_bytes = static_cast<double>(BATCH_BYTES) * num_timed;

  cudaMemcpy(&h_count, d_count, sizeof(unsigned long long),
             cudaMemcpyDeviceToHost);
  (void)h_count;

  result.batches_per_sec = num_timed / elapsed_s;
  result.gb_per_sec = (total_bytes / elapsed_s) / 1e9;
  result.alloc_time_ms =
      std::chrono::duration<double, std::milli>(t_alloc_end - t_alloc_start)
          .count();

  cudaFree(d_count);
  free_host_buffer(buf);
  return result;
}

} // namespace

std::vector<PipelineConfig> benchmark_matrix(size_t spsc_queue_count) {
  return {
      {"pageable+memcpy", AllocMode::Pageable, 0},
      {"pinned+mapped", AllocMode::PinnedMapped, 0},
      {"spsc(1 queue)", AllocMode::SPSC, 1},
      {"spsc(N queues)", AllocMode::SPSC, spsc_queue_count},
  };
}

void print_result(const PipelineConfig &cfg, const BenchResult &r) {
  if (cfg.alloc_mode == AllocMode::SPSC) {
    std::printf("%-22s %12.0f pkts/sec  %6.2f GB/s  errors=%llu\n", cfg.name,
                r.packets_per_sec, r.gb_per_sec, r.validation_errors);
  } else {
    std::printf("%-22s %8.2f batches/sec  %6.2f GB/s  alloc=%.3f ms\n",
                cfg.name, r.batches_per_sec, r.gb_per_sec, r.alloc_time_ms);
  }
}

int main(int argc, char **argv) {
  cudaSetDeviceFlags(cudaDeviceMapHost);
  cudaFree(0);

  constexpr int NUM_WARMUP_BATCHES = 10;
  constexpr int NUM_TIMED_BATCHES = 100;
  constexpr double SPSC_WARMUP_SECONDS = 0.5;
  constexpr double SPSC_TIMED_SECONDS = 2.0;

  bool benchmark_mode = false;
  size_t queues = 4;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--benchmark") {
      benchmark_mode = true;
    } else if (arg.rfind("--queues=", 0) == 0) {
      queues = std::stoul(arg.substr(std::strlen("--queues=")));
    }
  }

  if (benchmark_mode) {
    for (const auto &cfg : benchmark_matrix(queues)) {
      BenchResult r =
          cfg.alloc_mode == AllocMode::SPSC
              ? run_spsc_pipeline(cfg.num_queues, SPSC_WARMUP_SECONDS,
                                  SPSC_TIMED_SECONDS)
              : run_batch_pipeline(cfg, NUM_WARMUP_BATCHES, NUM_TIMED_BATCHES);
      print_result(cfg, r);
    }
  } else {
    PipelineConfig current{"current (spsc)", AllocMode::SPSC, queues};
    BenchResult r = run_spsc_pipeline(current.num_queues, SPSC_WARMUP_SECONDS,
                                      SPSC_TIMED_SECONDS);
    print_result(current, r);
  }

  return 0;
}