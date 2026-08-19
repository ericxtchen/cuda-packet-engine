#ifndef PACKET_ENVELOPE_H
#define PACKET_ENVELOPE_H

#include <cstddef>

constexpr size_t MAX_MTU_BYTES = 1518;
constexpr size_t SLOT_SIZE = 1536;

constexpr size_t BATCH_PACKETS = 65536; // 64K packets per batch
constexpr size_t BATCH_BYTES = BATCH_PACKETS * SLOT_SIZE; // ~100 MB per batch

#endif