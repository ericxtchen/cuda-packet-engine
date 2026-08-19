#ifndef PACKET_ENVELOPE_H
#define PACKET_ENVELOPE_H

#include <cstddef>
#include <cstdint>

constexpr size_t MAX_MTU_BYTES = 1518;
constexpr size_t SLOT_SIZE = 1536;

constexpr size_t BATCH_PACKETS = 65536; // 64K packets per batch
constexpr size_t BATCH_BYTES = BATCH_PACKETS * SLOT_SIZE; // ~100 MB per batch

constexpr size_t MIN_PAYLOAD = 18;
constexpr size_t MAX_PAYLOAD = 1472;

#pragma pack(push, 1)
struct EthernetHeader {
  uint8_t dest_mac[6];
  uint8_t src_mac[6];
  uint16_t ethertype; // 0x0800 for IPv4
};

struct IPv4Header {
  uint8_t version_ihl;
  uint8_t tos;
  uint16_t total_length;
  uint16_t id;
  uint16_t flags_fragment;
  uint8_t ttl;
  uint8_t protocol; // 0x11 for UDP
  uint16_t checksum;
  uint32_t src_ip;
  uint32_t dest_ip;
};

struct UDPHeader {
  uint16_t src_port;
  uint16_t dest_port;
  uint16_t length;
  uint16_t checksum;
};

struct PacketFrame {
  EthernetHeader eth;
  IPv4Header ip;
  UDPHeader udp;
  uint8_t payload[SLOT_SIZE - sizeof(EthernetHeader) - sizeof(IPv4Header) -
                  sizeof(UDPHeader)];
};
#pragma pack(pop)

#endif