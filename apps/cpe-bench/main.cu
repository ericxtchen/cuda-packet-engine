#include <chrono>
#include <cstdio>
#include <vector>

#include "cpe/generator.hpp"
#include "cpe/ingest_kernel.cuh"
#include "cpe/packet_envelope.hpp"

using clock_type = std::chrono::steady_clock;

int main(int argc, char **argv) {
  constexpr int NUM_WARMUP_BATCHES = 10;
  constexpr int NUM_TIMED_BATCHES = 100;

  // Force CUDA context creation now
  cudaFree(0);

  std::vector<uint8_t> buffer(BATCH_BYTES);

  unsigned long long h_count = 0;
  unsigned long long *d_count = nullptr;
  uint8_t *d_buf = nullptr;
  cudaMalloc((void **)&d_buf, BATCH_BYTES);
  cudaMalloc((void **)&d_count, sizeof(unsigned long long));

  int threadsPerBlock = 256;
  int blocksPerGrid = (BATCH_PACKETS + threadsPerBlock - 1) / threadsPerBlock;

  auto run_batch = [&]() {
    generate_traffic(buffer, BATCH_PACKETS);

    // Blocking cudaMemcpy per batch
    cudaMemcpy(d_buf, buffer.data(), BATCH_BYTES, cudaMemcpyHostToDevice);

    cudaMemset(d_count, 0, sizeof(unsigned long long));
    count_packets<<<blocksPerGrid, threadsPerBlock>>>(d_buf, BATCH_PACKETS,
                                                      d_count);
    cudaDeviceSynchronize();
  };

  // Warm-up pays for first-kernel-launch driver overhead,
  // first-touch page faults on the host buffer, and GPU clocks ramping up
  // to their boost state.
  for (int i = 0; i < NUM_WARMUP_BATCHES; ++i) {
    run_batch();
  }

  // --- Timed region ---
  auto t_start = clock_type::now();
  for (int batch = 0; batch < NUM_TIMED_BATCHES; ++batch) {
    run_batch();
  }
  auto t_end = clock_type::now();

  double elapsed_s = std::chrono::duration<double>(t_end - t_start).count();
  double total_bytes = static_cast<double>(BATCH_BYTES) * NUM_TIMED_BATCHES;
  double gb_per_sec = (total_bytes / elapsed_s) / 1e9;
  double batches_per_sec = NUM_TIMED_BATCHES / elapsed_s;

  cudaMemcpy(&h_count, d_count, sizeof(unsigned long long),
             cudaMemcpyDeviceToHost);

  std::printf(
      "M0 baseline: %.2f batches/sec, %.2f GB/s (last batch count=%llu)\n",
      batches_per_sec, gb_per_sec, h_count);

  cudaFree(d_buf);
  cudaFree(d_count);
  return 0;
}
