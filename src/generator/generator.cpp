// This is the source code for our packet generator
#include "cpe/generator.hpp"
#include "cpe/packet_envelope.hpp"
#include <arpa/inet.h>

// Calculate checksum via one's complement
uint16_t compute_ipv4_checksum(const void *vdata, size_t length) {
  const uint16_t *data = static_cast<const uint16_t *>(vdata);
  uint32_t acc = 0;

  for (size_t i = 0; i < length / 2; ++i) {
    acc += data[i]; // Sum 16-bit words directly in network byte order
  }

  // Fold 32-bit sum into 16 bits
  while (acc >> 16) {
    acc = (acc & 0xFFFF) + (acc >> 16);
  }

  return static_cast<uint16_t>(~acc); // One's complement
}

void generate_traffic(uint8_t *buf, size_t num_packets) {
  for (size_t i = 0; i < num_packets; ++i) {
    // variable payload size based on i
    const uint16_t payload_size =
        MIN_PAYLOAD + (i % (MAX_PAYLOAD - MIN_PAYLOAD + 1));
    // Calculate slot offset and cast to PacketFrame pointer
    uint8_t *slot_start = buf + (i * SLOT_SIZE);
    PacketFrame *frame = reinterpret_cast<PacketFrame *>(slot_start);

    // Populate Ethernet Header
    frame->eth.dest_mac[0] = 0x02; // Locally administered MAC
    frame->eth.dest_mac[1] = 0x00;
    frame->eth.dest_mac[2] = 0x00;
    frame->eth.dest_mac[3] = 0x00;
    frame->eth.dest_mac[4] = 0x00;
    frame->eth.dest_mac[5] = 0x01;

    frame->eth.src_mac[0] = 0x02;
    frame->eth.src_mac[1] = 0x00;
    frame->eth.src_mac[2] = 0x00;
    frame->eth.src_mac[3] = 0x00;
    frame->eth.src_mac[4] = 0x00;
    frame->eth.src_mac[5] = 0x02;

    frame->eth.ethertype = htons(0x0800); // 0x0800 = IPv4

    // Populate IPv4 Header
    frame->ip.version_ihl = 0x45; // Version 4, Header Length 5 (20 bytes)
    frame->ip.tos = 0;
    frame->ip.total_length =
        htons(sizeof(IPv4Header) + sizeof(UDPHeader) + payload_size);
    frame->ip.id = htons(static_cast<uint16_t>(i & 0xFFFF));
    frame->ip.flags_fragment = 0;
    frame->ip.ttl = 64;
    frame->ip.protocol = 17; // 17 = UDP
    frame->ip.checksum = 0;  // MUST zero out before calculating checksum!

    // Vary IPs slightly across packets to create multiple synthetic flows
    uint32_t src_ip = 0x0A000001 + (i % 256); // 10.0.0.1 to 10.0.0.256
    uint32_t dst_ip = 0xC0A80101;             // 192.168.1.1
    frame->ip.src_ip = htonl(src_ip);
    frame->ip.dest_ip = htonl(dst_ip);

    // Compute valid IPv4 header checksum
    frame->ip.checksum =
        compute_ipv4_checksum(&(frame->ip), sizeof(IPv4Header));

    // Populate UDP Header
    frame->udp.src_port = htons(static_cast<uint16_t>(1024 + (i % 1000)));
    frame->udp.dest_port = htons(8080);
    frame->udp.length = htons(sizeof(UDPHeader) + payload_size);
    frame->udp.checksum = 0; // Optional in IPv4 UDP

    // Fill Payload with dummy pattern
    uint8_t *payload = slot_start + sizeof(EthernetHeader) +
                       sizeof(IPv4Header) + sizeof(UDPHeader);
    payload[0] = static_cast<uint8_t>(i & 0xFF); // Pattern for validation
  }
}