// This generates packets for the engine to process
#ifndef GENERATOR_H
#define GENERATOR_H

#include "packet_envelope.hpp"
#include "spsc_queue.cuh"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

// RealisticMix is original behavior (payload size cycles across
// the full MIN_PAYLOAD..MAX_PAYLOAD range, i.e. 64B..1518B frames).
// Fixed64 pins every packet to MIN_PAYLOAD, producing an all-64B run.
// This is to isolate how much of the throughput cost
// is warp divergence from variable-length packets versus the parsing work
// itself.
enum class PacketSizeMode { RealisticMix, Fixed64 };

PacketFrame build_packet(uint64_t i,
                         PacketSizeMode mode = PacketSizeMode::RealisticMix);
void producer_loop(SPSC_Queue *queue, std::atomic<bool> *running,
                   PacketSizeMode mode = PacketSizeMode::RealisticMix);
std::vector<std::thread>
start_generators(std::vector<SPSC_Queue *> &queues, std::atomic<bool> *running,
                 PacketSizeMode mode = PacketSizeMode::RealisticMix);

void generate_traffic(uint8_t *buf, size_t num_packets,
                      PacketSizeMode mode = PacketSizeMode::RealisticMix);
uint16_t compute_ipv4_checksum(const void *vdata, size_t length);

#endif