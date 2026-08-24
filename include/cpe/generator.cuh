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

PacketFrame build_packet(uint64_t i);
void producer_loop(SPSC_Queue *queue, std::atomic<bool> *running);
std::vector<std::thread> start_generators(std::vector<SPSC_Queue *> &queues,
                                          std::atomic<bool> *running);

void generate_traffic(uint8_t *buf, size_t num_packets);
uint16_t compute_ipv4_checksum(const void *vdata, size_t length);

#endif