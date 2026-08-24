# cuda-packet-engine

This project is a synthetic, high-throughput GPU packet processing engine that offlaods networking tasks from the NIC to the GPU. CPE (cuda-packet-engine) eliminates host-side packet processing bottlenecks by leveraging zero-copy host-mapped memory, thread-pinned CPU traffic generators, and system-scope PCIe atomics.

Running this with `--benchmark` so far compares 3 different approaches for doing the same packet processing approach: 

1. Using a bounce buffer that uses `memcpy` to transfer the packets to the GPU
2. Using a batch processing method with a set pinned buffer with `cudaHostAllocMapped` and `cudaHostAllocWriteCombined`
3. Using Single-Processor-Single-Consumer (SPSC) Queues pinned to distinct CPU cores, with each thread writing to it's own ring buffer to remove contention. The SPSC Queue `head` and `tail` pointers are on different cache lines to avoid false sharing. It also uses `cuda::atomic_ref<uint64_t, cuda::thread_scope_system>` to do release/acquire memory ordering across the PCIe bus. GPU consumer threads continuously poll mapped host memory (`cudaHostAllocMapped`) for incoming doorbells.