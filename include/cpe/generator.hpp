// This generates packets for the engine to process
#ifndef GENERATOR_H
#define GENERATOR_H

#include <cstdint>
#include <vector>

void generate_traffic(std::vector<uint8_t> &buf, size_t num_packets);

#endif