// SPDX-License-Identifier: Apache-2.0
// SparkUrma: Application-layer chunk protocol for >4KB block transport.
//
// Tier S has a ~4KB reliable payload limit per single URMA WR.
// All blocks >4KB are split into <=4KB chunks with metadata headers,
// transmitted as individual URMA operations, and reassembled at the
// receiver with CRC32 integrity checks.

#ifndef SPARK_URMA_CHUNK_PROTOCOL_H
#define SPARK_URMA_CHUNK_PROTOCOL_H

#include <cstdint>
#include <cstddef>
#include <vector>
#include <string>
#include <unordered_map>
#include <chrono>

namespace spark_urma {

// ---- Chunk header (packed binary, placed in registered buffer) ----
// Total size: 40 bytes
#pragma pack(push, 1)
struct ChunkHeader {
    uint32_t magic;           // 0x55524D41 ("URMA")
    uint8_t  version;         // protocol version = 1
    uint8_t  operation;       // 0=WRITE, 1=READ_RESPONSE
    uint16_t reserved;
    uint64_t block_id;        // unique block identifier
    uint64_t request_id;      // transport-level request id
    uint32_t chunk_index;     // 0-based index within block
    uint32_t total_chunks;    // total number of chunks for this block
    uint32_t payload_length;  // actual data bytes in this chunk (<=max_chunk)
    uint32_t block_length;    // total block data length
    uint32_t chunk_checksum;  // CRC32 of this chunk's payload
    uint32_t block_checksum;  // CRC32 of the entire block data (same in all chunks)
};
#pragma pack(pop)

static_assert(sizeof(ChunkHeader) == 48, "ChunkHeader must be 48 bytes");

// Magic value
static constexpr uint32_t CHUNK_MAGIC = 0x55524D41;  // "URMA"

// Operation codes
static constexpr uint8_t CHUNK_OP_WRITE = 0;
static constexpr uint8_t CHUNK_OP_READ_RESPONSE = 1;

// ---- CRC32 (IEEE 802.3) ----
uint32_t crc32(const void* data, size_t length);
uint32_t crc32_combine(uint32_t crc, const void* data, size_t length);

// ---- Chunk Assembler (receiver side) ----
class ChunkAssembler {
public:
    static constexpr size_t MAX_PENDING_BLOCKS = 4096;
    static constexpr size_t MAX_PENDING_BYTES = 1ULL << 30;

    struct BlockState {
        uint64_t block_id;
        uint32_t total_chunks;
        uint32_t block_length;
        uint32_t expected_checksum;
        std::vector<bool> received_chunks;
        std::vector<uint8_t> data;
        uint32_t chunks_received = 0;
        bool complete = false;
        bool checksum_ok = false;
        std::chrono::steady_clock::time_point last_activity;
    };

    // Start assembling a new block. Returns false if block_id already tracked.
    bool start_block(uint64_t block_id, uint32_t total_chunks,
                     uint32_t block_length, uint32_t expected_checksum);

    // Add a chunk. Returns true if the block is now complete.
    bool add_chunk(const ChunkHeader& header, const uint8_t* payload);

    // Check if a specific block is complete and valid.
    bool is_complete(uint64_t block_id) const;
    bool is_valid(uint64_t block_id) const;

    // Get assembled data (valid only when complete and checksum ok).
    const std::vector<uint8_t>* get_data(uint64_t block_id) const;

    // Remove a completed block and return its data.
    std::vector<uint8_t> take_data(uint64_t block_id);

    // Clean up timed-out or completed blocks.
    void cleanup_stale(uint64_t timeout_sec = 60);

    size_t pending_blocks() const { return blocks_.size(); }

private:
    std::unordered_map<uint64_t, BlockState> blocks_;
    size_t pending_bytes_ = 0;
};

// ---- Chunk Splitter (sender side) ----
class ChunkSplitter {
public:
    // Split a data buffer into chunks. Returns chunk headers + payload pointers.
    struct Chunk {
        ChunkHeader header;
        const uint8_t* payload;  // points into original data
    };

    static std::vector<Chunk> split(
        const void* data, size_t total_length,
        uint64_t block_id, uint64_t request_id,
        uint32_t max_chunk_bytes,
        uint8_t op = CHUNK_OP_WRITE);
};

}  // namespace spark_urma

#endif  // SPARK_URMA_CHUNK_PROTOCOL_H
