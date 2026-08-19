#ifndef CPE_MEMORY_H
#define CPE_MEMORY_H

#include <cstddef>
#include <cstdint>

enum class AllocMode { Pageable, PinnedMapped };

struct HostBuffer {
  AllocMode mode;
  std::size_t bytes = 0;

  uint8_t *host_ptr = nullptr;   // where generate_traffic writes
  uint8_t *device_ptr = nullptr; // what the kernel reads

  HostBuffer() = default;

  HostBuffer(const HostBuffer &) = delete;
  HostBuffer &operator=(const HostBuffer &) = delete;

  HostBuffer(HostBuffer &&other) noexcept;
  HostBuffer &operator=(HostBuffer &&other) noexcept;

  ~HostBuffer();
};

// Allocates once..
HostBuffer allocate_host_buffer(AllocMode mode, std::size_t bytes);
void free_host_buffer(HostBuffer &buf);

// Pageable: issues a real cudaMemcpy H2D.
// PinnedMapped: a no-op. host_ptr and device_ptr already reference the
// same physical memory.
void sync_to_device(const HostBuffer &buf, std::size_t bytes);

#endif