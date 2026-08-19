// This is the source code for our packet generator
#include "cpe/generator.hpp"
#include "cpe/packet_envelope.hpp"

void generate_traffic(std::vector<uint8_t> &buf, size_t num_packets) {
  for (size_t i = 0; i < num_packets; ++i) {
    uint8_t *slot_start = buf.data() + (i * SLOT_SIZE);

    slot_start[0] = static_cast<uint8_t>(i & 0xFE);
  }
}