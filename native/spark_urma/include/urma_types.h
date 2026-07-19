// SPDX-License-Identifier: Apache-2.0
// SparkUrma: Extended type definitions for C++ URMA Transport.
// Depends on the official UMDK urma_types.h and urma_api.h.

#ifndef SPARK_URMA_TYPES_H
#define SPARK_URMA_TYPES_H

#include <cstdint>
#include <string>
#include <vector>
#include <cstring>

namespace spark_urma {

// ---- Transport configuration ----
struct TransportOptions {
    std::string device_name = "openurma0";
    std::string wire_role = "connect";    // Tier S wire role: "listen" or "connect"
    uint32_t queue_depth = 128;
    uint32_t poll_batch = 32;
    uint32_t request_timeout_ms = 30000;
    uint32_t max_chunk_bytes = 4096;  // Tier S safe limit
    bool strict_mode = true;
};

// ---- Registered memory region ----
struct RegisteredRegion {
    uint64_t handle = 0;
    void* address = nullptr;
    size_t length = 0;
    uint64_t remote_address = 0;
    uint64_t token = 0;
    uint64_t owner_epoch = 0;
};

// ---- Remote memory region descriptor ----
struct RemoteRegion {
    uint64_t remote_address = 0;
    uint64_t length = 0;
    uint64_t token = 0;
};

// ---- Completion result ----
struct CompletionResult {
    uint64_t request_id = 0;
    int status = 0;        // 0 = success, <0 = error
    size_t bytes = 0;
    std::string message;
};

enum class RequestState : int32_t {
    CREATED = 0,
    POSTED = 1,
    COMPLETED_SUCCESS = 2,
    COMPLETED_FAILED = 3,
    TIMED_OUT_PROVIDER_OUTSTANDING = 4,
    TIMED_OUT_DRAINED = 5,
    CANCELLED_PROVIDER_OUTSTANDING = 6,
    CANCELLED_DRAINED = 7,
};

struct ProcessRuntimeMetrics {
    uint64_t generation = 0;
    uint64_t init_calls = 0;
    uint64_t uninit_calls = 0;
    uint64_t active_transports = 0;
    bool initialized = false;
};

// ---- Endpoint descriptor exchanged via control channel ----
struct EndpointDescriptor {
    uint32_t protocol_version = 1;
    uint32_t uasid = 0;
    uint32_t jetty_id = 0;
    uint8_t eid[16] = {};
    uint64_t segment_address = 0;
    uint64_t segment_length = 0;
    uint32_t segment_token = 0;
    uint32_t max_chunk_bytes = 4096;
    uint32_t transport_mode = 0;    // URMA_TM_RC

    // Serialize to/from network byte order
    void to_network();
    void from_network();
};

// ---- Protocol versions ----
static constexpr uint32_t PROTOCOL_VERSION = 1;

// ---- Error codes ----
enum class ErrorCode : int32_t {
    SUCCESS = 0,
    ERR_INIT = -1,
    ERR_DEVICE_NOT_FOUND = -2,
    ERR_CONTEXT = -3,
    ERR_RESOURCE = -4,
    ERR_REGISTRATION = -5,
    ERR_CONNECTION = -6,
    ERR_TIMEOUT = -7,
    ERR_COMPLETION = -8,
    ERR_PROTOCOL = -9,
    ERR_INVALID_ARG = -10,
    ERR_CLOSED = -11,
    ERR_UNKNOWN_REQUEST = -12,
    ERR_ALREADY_CONSUMED = -13,
    ERR_OWNER = -14,
    ERR_BOUNDS = -15,
    ERR_BUSY = -16,
    ERR_INTERNAL = -99,
};

// ---- Status codes mapped from URMA CR status ----
enum class CrStatus : int32_t {
    OK = 0,
    WR_FLUSH_ERR = 1,
    LOCAL_PROTECTION_ERR = 2,
    REMOTE_ACCESS_ERR = 3,
    TRANSPORT_RETRY_EXC_ERR = 4,
    ABORT_ERR = 5,
};

}  // namespace spark_urma

#endif  // SPARK_URMA_TYPES_H
