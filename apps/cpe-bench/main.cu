#include <chrono>
#include <cstdio>
#include <string>
#include <vector>

#include "cpe/generator.hpp"
#include "cpe/ingest_kernel.cuh"
#include "cpe/memory.hpp"
#include "cpe/packet_envelope.hpp"

using clock_type = std::chrono::steady_clock;

struct PipelineConfig {
  const char *name;
  AllocMode alloc_mode;
};

struct BenchResult {
  double batches_per_sec = 0;
  double gb_per_sec = 0;
  double alloc_time_ms = 0;
};

BenchResult run_pipeline(const PipelineConfig &cfg, int num_warmup,
                         int num_timed) {
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

  // warmup cuda
  for (int i = 0; i < num_warmup; ++i)
    run_batch();

  // actually run the batches
  auto t_start = clock_type::now();
  for (int batch = 0; batch < num_timed; ++batch)
    run_batch();
  auto t_end = clock_type::now();

  double elapsed_s = std::chrono::duration<double>(t_end - t_start).count();
  double total_bytes = static_cast<double>(BATCH_BYTES) * num_timed;

  cudaMemcpy(&h_count, d_count, sizeof(unsigned long long),
             cudaMemcpyDeviceToHost);
  (void)h_count;

  BenchResult result;
  result.batches_per_sec = num_timed / elapsed_s;
  result.gb_per_sec = (total_bytes / elapsed_s) / 1e9;
  result.alloc_time_ms =
      std::chrono::duration<double, std::milli>(t_alloc_end - t_alloc_start)
          .count();

  cudaFree(d_count);

  return result;
}

// Add a new row to benchmark new configurations
std::vector<PipelineConfig> benchmark_matrix() {
  return {
      {"pageable+memcpy", AllocMode::Pageable},
      {"pinned+mapped", AllocMode::PinnedMapped},
  };
}

void print_result(const char *name, const BenchResult &r) {
  std::printf("%-22s %8.2f batches/sec  %6.2f GB/s  alloc=%.3f ms\n", name,
              r.batches_per_sec, r.gb_per_sec, r.alloc_time_ms);
}

int main(int argc, char **argv) {
  cudaSetDeviceFlags(cudaDeviceMapHost);
  cudaFree(0); // now safe to force context creation

  constexpr AllocMode CURRENT_ALLOC_MODE = AllocMode::PinnedMapped;

  constexpr int NUM_WARMUP_BATCHES = 10;
  constexpr int NUM_TIMED_BATCHES = 100;

  bool benchmark_mode = false;
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--benchmark")
      benchmark_mode = true;
  }

  if (benchmark_mode) {
    // Diagnostic / write-up mode: show every historically-interesting
    // config side by side, same process, same thermal state.
    for (const auto &cfg : benchmark_matrix()) {
      BenchResult r = run_pipeline(cfg, NUM_WARMUP_BATCHES, NUM_TIMED_BATCHES);
      print_result(cfg.name, r);
    }
  } else {
    PipelineConfig current{CURRENT_ALLOC_MODE == AllocMode::Pageable
                               ? "current (pageable)"
                               : "current (pinned+mapped)",
                           CURRENT_ALLOC_MODE};
    BenchResult r =
        run_pipeline(current, NUM_WARMUP_BATCHES, NUM_TIMED_BATCHES);
    print_result(current.name, r);
  }

  return 0;
}
