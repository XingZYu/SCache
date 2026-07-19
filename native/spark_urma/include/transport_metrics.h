// SPDX-License-Identifier: Apache-2.0
// SparkUrma: Transport metrics collection.

#ifndef SPARK_URMA_TRANSPORT_METRICS_H
#define SPARK_URMA_TRANSPORT_METRICS_H

#include <atomic>
#include <cstdint>

namespace spark_urma {

struct TransportMetrics {
    // Request counts
    std::atomic<uint64_t> submitted_requests{0};
    std::atomic<uint64_t> completed_requests{0};
    std::atomic<uint64_t> failed_requests{0};
    std::atomic<uint64_t> timed_out_requests{0};
    std::atomic<uint64_t> inflight_requests{0};
    // Provider work can remain outstanding after the API request times out.
    std::atomic<uint64_t> provider_outstanding_requests{0};
    std::atomic<uint64_t> provider_drained_requests{0};
    std::atomic<uint64_t> submitted_bytes{0};
    std::atomic<uint64_t> completed_bytes{0};
    std::atomic<uint64_t> late_completions{0};
    std::atomic<uint64_t> unknown_waits{0};

    // Per-operation counts
    std::atomic<uint64_t> write_requests{0};
    std::atomic<uint64_t> read_requests{0};
    std::atomic<uint64_t> send_requests{0};

    // Byte counts
    std::atomic<uint64_t> write_bytes{0};
    std::atomic<uint64_t> read_bytes{0};
    std::atomic<uint64_t> send_bytes{0};

    // Chunking
    std::atomic<uint64_t> chunked_blocks{0};
    std::atomic<uint64_t> chunks{0};
    std::atomic<uint64_t> logical_blocks{0};

    std::atomic<uint64_t> registered_regions{0};
    std::atomic<uint64_t> unregistered_regions{0};
    std::atomic<uint64_t> active_regions{0};
    std::atomic<uint64_t> imported_regions{0};
    std::atomic<uint64_t> unimported_regions{0};
    std::atomic<uint64_t> active_imports{0};
    std::atomic<uint64_t> init_calls{0};
    std::atomic<uint64_t> uninit_calls{0};
    std::atomic<uint64_t> active_transports{0};
    std::atomic<uint64_t> close_errors{0};

    // Errors
    std::atomic<uint64_t> checksum_errors{0};
    std::atomic<uint64_t> tcp_fallbacks{0};

    // Control channel
    std::atomic<uint64_t> tcp_control_bytes{0};
    std::atomic<uint64_t> tcp_payload_bytes{0};

    void record_write(uint64_t request_id, size_t bytes);
    void record_read(uint64_t request_id, size_t bytes);
    void record_send(uint64_t request_id, size_t bytes);
    void record_completion(uint64_t request_id, bool success);
    void record_timeout(uint64_t request_id);
    void record_provider_post(uint64_t request_id);
    void record_provider_drained(uint64_t request_id);
    void record_chunked_block(uint64_t num_chunks);
};

}  // namespace spark_urma

#endif  // SPARK_URMA_TRANSPORT_METRICS_H
