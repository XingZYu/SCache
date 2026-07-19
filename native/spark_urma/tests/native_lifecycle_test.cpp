// SPDX-License-Identifier: Apache-2.0
// SparkUrma: Minimal native lifecycle test for the C++ URMA Transport.
// Verifies: init → close, double close safety, and chunk protocol correctness.

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cassert>
#include <stdexcept>
#include "urma_transport.h"
#include "chunk_protocol.h"

int main() {
    int failed = 0;

    // ---- Test 1: Chunk protocol CRC32 ----
    {
        const char* test_data = "Hello, URMA!";
        uint32_t crc1 = spark_urma::crc32(test_data, strlen(test_data));
        uint32_t crc2 = spark_urma::crc32(test_data, strlen(test_data));
        if (crc1 == crc2 && crc1 != 0 && crc1 != 0xFFFFFFFF) {
            printf("PASS: CRC32 deterministic and non-trivial (0x%08X)\n", crc1);
        } else {
            printf("FAIL: CRC32\n");
            failed++;
        }
    }

    // ---- Test 2: Chunk Splitter ----
    {
        uint8_t data[8192];
        for (size_t i = 0; i < sizeof(data); i++) {
            data[i] = static_cast<uint8_t>(i & 0xFF);
        }

        auto chunks = spark_urma::ChunkSplitter::split(data, sizeof(data), 1, 100, 4096);
        if (chunks.size() == 2) {
            printf("PASS: ChunkSplitter produced %zu chunks for 8KB data\n", chunks.size());
        } else {
            printf("FAIL: Expected 2 chunks, got %zu\n", chunks.size());
            failed++;
        }
    }

    // ---- Test 3: Chunk Assembler ----
    {
        uint8_t data[4096];
        for (size_t i = 0; i < sizeof(data); i++) {
            data[i] = static_cast<uint8_t>((i * 7 + 13) & 0xFF);
        }

        auto chunks = spark_urma::ChunkSplitter::split(data, sizeof(data), 42, 200, 1024);
        spark_urma::ChunkAssembler assembler;
        assembler.start_block(42, static_cast<uint32_t>(chunks.size()),
                              sizeof(data), chunks[0].header.block_checksum);

        for (auto& c : chunks) {
            assembler.add_chunk(c.header, c.payload);
        }

        if (assembler.is_complete(42) && assembler.is_valid(42)) {
            auto* result = assembler.get_data(42);
            if (result && result->size() == sizeof(data) &&
                memcmp(result->data(), data, sizeof(data)) == 0) {
                printf("PASS: ChunkAssembler round-trip for 4KB in %zu chunks\n", chunks.size());
            } else {
                printf("FAIL: ChunkAssembler data mismatch\n");
                failed++;
            }
        } else {
            printf("FAIL: ChunkAssembler block not complete/valid\n");
            failed++;
        }
    }

    // ---- Test 4: uneven chunks, malformed metadata and stale cleanup ----
    {
        uint8_t data[5000];
        for (size_t i = 0; i < sizeof(data); i++) data[i] = static_cast<uint8_t>(i * 11);
        auto chunks = spark_urma::ChunkSplitter::split(data, sizeof(data), 43, 201, 4096);
        spark_urma::ChunkAssembler assembler;
        bool ok = chunks.size() == 2 &&
            assembler.start_block(43, static_cast<uint32_t>(chunks.size()), sizeof(data),
                                  chunks[0].header.block_checksum);
        auto malformed = chunks[0].header;
        malformed.payload_length--;
        ok = ok && !assembler.add_chunk(malformed, chunks[0].payload);
        // Arrival order is deliberately reversed.
        ok = ok && !assembler.add_chunk(chunks[1].header, chunks[1].payload);
        ok = ok && assembler.add_chunk(chunks[0].header, chunks[0].payload);
        const auto* result = assembler.get_data(43);
        ok = ok && result && result->size() == sizeof(data) &&
             std::memcmp(result->data(), data, sizeof(data)) == 0;
        assembler.cleanup_stale(0);
        ok = ok && assembler.pending_blocks() == 0;
        ok = ok && assembler.start_block(44, 2, 100, 0);
        assembler.cleanup_stale(0);
        ok = ok && assembler.pending_blocks() == 0;
        if (ok) printf("PASS: uneven chunk offsets + malformed rejection + stale cleanup\n");
        else { printf("FAIL: chunk boundary safety\n"); failed++; }
    }

    // ---- Test 5: Real transport registration, same-process reopen and runtime shutdown ----
    {
        try {
            spark_urma::TransportOptions opts;
            opts.device_name = "openurma0";
            const char* role = std::getenv("OPENURMA_WIRE_ROLE");
            opts.wire_role = role ? role : "connect";
            alignas(4096) uint8_t region[4096] = {};
            {
                spark_urma::UrmaTransport t;
                t.init(opts);
                auto registered = t.register_region(region, sizeof(region));
                t.unregister_region(registered.handle);
                t.close();
                t.close();
            }
            {
                spark_urma::UrmaTransport reopened;
                reopened.init(opts);
                reopened.close();
            }
            auto before = spark_urma::UrmaTransport::process_runtime_metrics();
            if (before.generation != 2 || before.init_calls != 1 ||
                before.uninit_calls != 0 || before.active_transports != 0 || !before.initialized) {
                throw std::runtime_error("runtime metrics before shutdown mismatch");
            }
            spark_urma::UrmaTransport::shutdown_process_runtime();
            auto after = spark_urma::UrmaTransport::process_runtime_metrics();
            if (after.init_calls != 1 || after.uninit_calls != 1 ||
                after.active_transports != 0 || after.initialized) {
                throw std::runtime_error("runtime metrics after shutdown mismatch");
            }
            printf("PASS: Transport registration + double close + same-process reopen + runtime shutdown\n");
        } catch (const std::exception& e) {
            printf("FAIL: Transport lifecycle (%s)\n", e.what());
            failed++;
        }
    }

    printf("\n=== %d test(s) failed ===\n", failed);
    return failed ? 1 : 0;
}
