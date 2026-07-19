// SPDX-License-Identifier: Apache-2.0
// SparkUrma: C++ URMA Transport with single owner-thread safety.
// All URMA/provider/SystemC calls are serialized via internal mutex.

#ifndef SPARK_URMA_TRANSPORT_H
#define SPARK_URMA_TRANSPORT_H

#include <memory>
#include <string>
#include <vector>
#include <mutex>
#include <deque>
#include <unordered_map>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <functional>
#include <future>
#include <chrono>

#include "urma_types.h"
#include "transport_metrics.h"

struct urma_context;
struct urma_jetty;
struct urma_jfc;
struct urma_jfr;
struct urma_target_seg;
struct urma_target_jetty;
typedef struct urma_context   urma_context_t;
typedef struct urma_jetty     urma_jetty_t;
typedef struct urma_jfc       urma_jfc_t;
typedef struct urma_jfr       urma_jfr_t;
typedef struct urma_target_seg urma_target_seg_t;
typedef struct urma_target_jetty urma_target_jetty_t;

namespace spark_urma {

// 单次请求的最大安全字节数（Tier S 限制）
static constexpr size_t MAX_SINGLE_REQUEST_BYTES = 4096;

enum class TransportState { NEW, INITIALIZED, CONNECTED, RUNNING, CLOSING, CLOSED };

class UrmaTransport {
public:
    UrmaTransport();
    ~UrmaTransport() noexcept;

    UrmaTransport(const UrmaTransport&) = delete;
    UrmaTransport& operator=(const UrmaTransport&) = delete;
    UrmaTransport(UrmaTransport&&) = delete;
    UrmaTransport& operator=(UrmaTransport&&) = delete;

    // ---- Lifecycle (caller thread, before owner loop starts) ----
    void init(const TransportOptions& options);
    void connect(const EndpointDescriptor& remote);
    void close();
    static void shutdown_process_runtime();
    static ProcessRuntimeMetrics process_runtime_metrics();

    TransportState state() const { return state_; }

    // ---- Memory registration (thread-safe via owner_mutex_) ----
    RegisteredRegion register_region(void* address, size_t length);
    void unregister_region(uint64_t handle);
    RegisteredRegion registered_region(uint64_t handle) const;

    // ---- 裸数据操作：严格限制 <= 4096B ----
    uint64_t write(const RemoteRegion& remote, const void* source,
                   size_t length, size_t remote_offset = 0);
    uint64_t read(const RemoteRegion& remote, void* destination,
                  size_t length, size_t remote_offset = 0);
    uint64_t send(const void* message, size_t length);

    // ---- Block API：自动分片，支持任意大小 ----
    uint64_t writeBlock(const RemoteRegion& remote, const void* source,
                        size_t total_length);
    uint64_t readBlock(const RemoteRegion& remote, void* destination,
                       size_t total_length);

    // ---- Legacy chunked (内部使用) ----
    uint64_t write_chunked(const RemoteRegion& remote, const void* source,
                           size_t total_length);
    uint64_t read_chunked(const RemoteRegion& remote, void* destination,
                          size_t total_length);

    // ---- Completion (thread-safe: polls JFC under owner_mutex_) ----
    CompletionResult wait(uint64_t request_id, uint32_t timeout_ms);

    // ---- Accessors ----
    TransportMetrics& metrics() { return metrics_; }
    const EndpointDescriptor& local_endpoint() const { return local_ep_; }
    TransportOptions options() const { return options_; }
    std::thread::id owner_thread_id() const { return owner_tid_; }

private:
    uint64_t next_request_id();

    // Lock that serializes ALL provider/SystemC calls
    mutable std::mutex owner_mutex_;
    std::thread::id owner_tid_;
    void assert_owner_or_lock();

    // URMA handles
    urma_context_t* ctx_ = nullptr;
    urma_jetty_t* jetty_ = nullptr;
    urma_jfc_t* jfc_ = nullptr;
    urma_jfr_t* jfr_ = nullptr;
    urma_target_seg_t* local_seg_ = nullptr;
    urma_target_jetty_t* remote_jetty_ = nullptr;
    struct ImportedRegionState {
        RemoteRegion region;
        urma_target_seg_t* segment = nullptr;
        uint64_t inflight = 0;
        uint64_t last_used = 0;
    };
    std::vector<ImportedRegionState> imported_regions_;
    uint64_t import_clock_ = 0;
    static constexpr size_t MAX_REMOTE_IMPORTS = 1024;

    TransportOptions options_;
    TransportState state_ = TransportState::NEW;
    EndpointDescriptor local_ep_;
    EndpointDescriptor remote_ep_;

    struct LocalRegionState {
        RegisteredRegion region;
        urma_target_seg_t* segment = nullptr;
        uint64_t inflight = 0;
        bool registered = true;
    };
    std::unordered_map<uint64_t, LocalRegionState> registered_regions_;
    uint64_t next_region_handle_ = 1;

    struct RequestRecord {
        uint64_t request_id;
        bool is_write = false, is_read = false, is_send = false;
        bool is_aggregate = false;
        size_t bytes = 0;
        size_t completed_bytes = 0;
        RequestState state = RequestState::CREATED;
        int status = 0;
        std::string message;
        uint64_t local_region_handle = 0;
        urma_target_seg_t* remote_segment = nullptr;
        bool local_ref_released = false;
        bool provider_outstanding = false;
        std::vector<uint64_t> children;
    };
    std::unordered_map<uint64_t, RequestRecord> requests_;
    std::deque<uint64_t> terminal_order_;
    static constexpr size_t MAX_TERMINAL_RESULTS = 65536;
    std::condition_variable completion_cv_;
    std::atomic<uint64_t> request_seq_{1};
    TransportMetrics metrics_;

    void* local_buffer_ = nullptr;
    size_t local_buffer_size_ = 0;

    uint64_t owner_epoch_ = 0;
    bool owns_runtime_slot_ = false;

    static bool range_contains(uint64_t base, uint64_t length,
                               uint64_t address, uint64_t bytes);
    LocalRegionState& find_local_region_locked(const void* address, size_t length);
    ImportedRegionState& find_or_import_remote_locked(const RemoteRegion& remote);
    uint64_t post_rw_locked(bool write_op, const RemoteRegion& remote,
                            void* local, size_t length, size_t remote_offset);
    void finalize_locked(RequestRecord& request, RequestState state, int status,
                         size_t completed_bytes, const std::string& message);
    void provider_drained_locked(RequestRecord& request);
    void poll_once_locked();
    void refresh_aggregates_locked();
    CompletionResult result_locked(const RequestRecord& request) const;
    void trim_terminal_cache_locked();
    void cleanup_locked(std::vector<std::string>& errors);
};

}  // namespace spark_urma
#endif
