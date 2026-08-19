#include "cpe/memory.hpp"
#include <cuda_runtime.h>

HostBuffer::~HostBuffer() {
  if (host_ptr)
    free_host_buffer(*this);
}

HostBuffer::HostBuffer(HostBuffer &&other) noexcept
    : mode(other.mode), bytes(other.bytes), host_ptr(other.host_ptr),
      device_ptr(other.device_ptr) {
  other.host_ptr = nullptr;
  other.device_ptr = nullptr;
  other.bytes = 0;
}

HostBuffer &HostBuffer::operator=(HostBuffer &&other) noexcept {
  if (this != &other) {
    if (host_ptr)
      free_host_buffer(*this);
    mode = other.mode;
    bytes = other.bytes;
    host_ptr = other.host_ptr;
    device_ptr = other.device_ptr;
    other.host_ptr = nullptr;
    other.device_ptr = nullptr;
    other.bytes = 0;
  }
  return *this;
}

HostBuffer allocate_host_buffer(AllocMode mode, std::size_t bytes) {
  HostBuffer host_buffer;
  host_buffer.mode = mode;
  host_buffer.bytes = bytes;
  switch (mode) {
  case AllocMode::Pageable:
    host_buffer.host_ptr = new uint8_t[bytes];
    cudaMalloc(reinterpret_cast<void **>(&host_buffer.device_ptr), bytes);
    break;
  case AllocMode::PinnedMapped:
    unsigned int flags = cudaHostAllocMapped | cudaHostAllocWriteCombined;
    // cudaSetDeviceFlags(cudaDeviceMapHost); this should be executed once
    // during init
    cudaHostAlloc(reinterpret_cast<void **>(&host_buffer.host_ptr), bytes,
                  flags);
    cudaHostGetDevicePointer(reinterpret_cast<void **>(&host_buffer.device_ptr),
                             host_buffer.host_ptr, 0);
  }

  return host_buffer;
}

void free_host_buffer(HostBuffer &buf) {
  switch (buf.mode) {
  case AllocMode::Pageable:
    delete[] buf.host_ptr;
    cudaFree(buf.device_ptr);
    break;
  case AllocMode::PinnedMapped:
    cudaFreeHost(buf.host_ptr);
  }

  buf.device_ptr = nullptr;
  buf.host_ptr = nullptr;
  buf.bytes = 0;
}

void sync_to_device(const HostBuffer &buf, std::size_t bytes) {
  switch (buf.mode) {
  case AllocMode::Pageable:
    cudaMemcpy(buf.device_ptr, buf.host_ptr, bytes, cudaMemcpyHostToDevice);
    break;
  case AllocMode::PinnedMapped:
  }
}