// This generates packets for the engine to process
#ifndef GENERATOR_H
#define GENERATOR_H

#include <cstddef>
#include <cstdint>

void generate_traffic(uint8_t *buf, size_t num_packets);
uint16_t compute_ipv4_checksum(const void *vdata, size_t length);

#endif