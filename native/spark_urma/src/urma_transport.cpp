// SPDX-License-Identifier: Apache-2.0
#include "urma_transport.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <dlfcn.h>

extern "C" {
#include "urma_api.h"
#include "urma_types.h"
#if __has_include("openurma_vdev_q5.h")
// The shipped Q8 ABI header is C-only and uses _Static_assert, which is not
// accepted by the C++ compiler.  Keep the stable 64-byte MR response layout
// local and call the optional helper through its C ABI instead of including
// the C header in this C++ translation unit.
struct spark_openurma_vdev_mr_resp {
    uint32_t magic;
    uint16_t abi_major;
    uint16_t abi_minor;
    uint64_t generation;
    uint64_t va;
    uint64_t len;
    uint32_t token_id;
    uint32_t access;
    uint32_t pinned_pages;
    uint32_t eid_index;
    uint32_t state;
    uint32_t reserved0;
    uint64_t reserved[1];
};
static_assert(sizeof(spark_openurma_vdev_mr_resp) == 64,
              "Q8 MR response ABI size");
int openurma_vdev_q5_mr_info(urma_target_seg_t *seg,
                             spark_openurma_vdev_mr_resp *info);
#define SPARK_URMA_HAS_VDEV_MR_INFO 1
#endif
}

namespace spark_urma {
namespace {
std::mutex g_process_mutex;
bool g_transport_active = false;
bool g_runtime_initialized = false;
bool g_runtime_shutdown = false;
uint64_t g_transport_epoch = 0;
uint64_t g_runtime_init_calls = 0;
uint64_t g_runtime_uninit_calls = 0;

#if defined(SPARK_URMA_HAS_VDEV_MR_INFO)
using SparkUrmaVdevMrInfoFn = int (*)(urma_target_seg_t *, spark_openurma_vdev_mr_resp *);

SparkUrmaVdevMrInfoFn resolve_vdev_mr_info() {
    void* symbol = dlsym(RTLD_DEFAULT, "openurma_vdev_q5_mr_info");
    if (symbol == nullptr) {
        // liburma normally loads the provider before SCache starts.  Some
        // loaders keep provider symbols local, so make the already-loaded
        // plugin visible without taking ownership of a second provider.
        void* handle = dlopen("liburma_openurma_vdev.so", RTLD_NOW | RTLD_GLOBAL | RTLD_NOLOAD);
        if (handle != nullptr) symbol = dlsym(handle, "openurma_vdev_q5_mr_info");
    }
    return reinterpret_cast<SparkUrmaVdevMrInfoFn>(symbol);
}
#endif

std::string join_errors(const std::vector<std::string>& errors) {
    std::ostringstream out;
    for (size_t i = 0; i < errors.size(); ++i) {
        if (i != 0) out << "; ";
        out << errors[i];
    }
    return out.str();
}
}  // namespace

#define LOCK() std::lock_guard<std::mutex> _lk(owner_mutex_)

UrmaTransport::UrmaTransport() = default;

UrmaTransport::~UrmaTransport() noexcept {
    try {
        if (state_ != TransportState::CLOSED) close();
    } catch (...) {
        // Destructors cannot report errors. Explicit close is the tested contract.
    }
}

bool UrmaTransport::range_contains(uint64_t base, uint64_t length,
                                   uint64_t address, uint64_t bytes) {
    if (address < base || bytes > length) return false;
    return address - base <= length - bytes;
}

void UrmaTransport::init(const TransportOptions& options) {
    LOCK();
    if (state_ != TransportState::NEW) throw std::runtime_error("ERR_INIT: already initialized");
    if (options.device_name.empty() || options.queue_depth == 0 ||
        options.max_chunk_bytes == 0) {
        throw std::runtime_error("ERR_INVALID_ARG: invalid device/queue/maxChunkBytes");
    }

    const char* launcher_role = std::getenv("OPENURMA_WIRE_ROLE");
    if (launcher_role == nullptr || *launcher_role == '\0') {
        throw std::runtime_error("ERR_INIT: OPENURMA_WIRE_ROLE must be set by launcher");
    }
    if (!options.wire_role.empty() && options.wire_role != launcher_role) {
        throw std::runtime_error("ERR_INIT: launcher OPENURMA_WIRE_ROLE conflicts with expected role");
    }

    options_ = options;
    owner_tid_ = std::this_thread::get_id();
    try {
        {
            std::lock_guard<std::mutex> process_lock(g_process_mutex);
            if (g_transport_active) {
                throw std::runtime_error("ERR_BUSY: one active UrmaTransport is allowed per process");
            }
            if (g_runtime_shutdown) {
                throw std::runtime_error("ERR_CLOSED: process URMA runtime was shut down");
            }
            if (!g_runtime_initialized) {
                urma_init_attr_t init_attr = {};
                if (urma_init(&init_attr) != URMA_SUCCESS) {
                    throw std::runtime_error("urma_init failed");
                }
                g_runtime_initialized = true;
                ++g_runtime_init_calls;
                metrics_.init_calls.fetch_add(1);
            }
            g_transport_active = true;
            owns_runtime_slot_ = true;
            owner_epoch_ = ++g_transport_epoch;
        }
        metrics_.active_transports.fetch_add(1);

        urma_device_t* device = urma_get_device_by_name(
            const_cast<char*>(options_.device_name.c_str()));
        if (device == nullptr) throw std::runtime_error("device not found: " + options_.device_name);
        urma_device_attr_t device_attr = {};
        if (urma_query_device(device, &device_attr) != URMA_SUCCESS) {
            throw std::runtime_error("urma_query_device failed");
        }
        const uint64_t provider_limit = std::min<uint64_t>(
            device_attr.dev_cap.max_msg_size,
            std::min<uint64_t>(device_attr.dev_cap.max_read_size,
                               device_attr.dev_cap.max_write_size));
        if (provider_limit == 0) {
            throw std::runtime_error(
                "provider advertised a zero READ/WRITE/message limit");
        }
        provider_max_raw_operation_bytes_ = static_cast<uint32_t>(
            std::min<uint64_t>(
                provider_limit, std::numeric_limits<uint32_t>::max()));
        local_max_raw_operation_bytes_ =
            std::min<uint32_t>(
                options_.max_chunk_bytes, provider_max_raw_operation_bytes_);
        effective_max_raw_operation_bytes_ =
            local_max_raw_operation_bytes_;
        if (local_max_raw_operation_bytes_ == 0) {
            throw std::runtime_error(
                "negotiated local raw operation limit is zero");
        }
        std::fprintf(stderr,
            "SPARK_URMA_CAPABILITY configuredMax=%u providerMax=%u "
            "localMax=%u wireMtuEnum=%u\n",
            options_.max_chunk_bytes, provider_max_raw_operation_bytes_,
            local_max_raw_operation_bytes_,
            static_cast<unsigned>(device_attr.port_attr[0].active_mtu));
        ctx_ = urma_create_context(device, 0);
        if (ctx_ == nullptr) throw std::runtime_error("urma_create_context failed");

        urma_jfc_cfg_t jfc_cfg = {};
        jfc_cfg.depth = options_.queue_depth;
        jfc_ = urma_create_jfc(ctx_, &jfc_cfg);
        if (jfc_ == nullptr) throw std::runtime_error("urma_create_jfc failed");

        // Q8 exposes post_jfs_wr (not post_jetty_send_wr). Keep a standalone
        // JFS for data-plane posts; the Jetty remains the endpoint.
        urma_jfs_cfg_t jfs_cfg = {};
        jfs_cfg.depth = options_.queue_depth;
        jfs_cfg.trans_mode = URMA_TM_RC;
        jfs_cfg.jfc = jfc_;
        jfs_ = urma_create_jfs(ctx_, &jfs_cfg);
        if (jfs_ == nullptr) throw std::runtime_error("urma_create_jfs failed");

        urma_jfr_cfg_t jfr_cfg = {};
        jfr_cfg.depth = options_.queue_depth;
        jfr_cfg.trans_mode = URMA_TM_RC;
        jfr_cfg.jfc = jfc_;
        jfr_cfg.token_value.token = 0xDEADBEEF;
        jfr_ = urma_create_jfr(ctx_, &jfr_cfg);
        if (jfr_ == nullptr) throw std::runtime_error("urma_create_jfr failed");

        urma_jetty_cfg_t jetty_cfg = {};
        jetty_cfg.jfs_cfg.depth = options_.queue_depth;
        jetty_cfg.jfs_cfg.trans_mode = URMA_TM_RC;
        jetty_cfg.jfs_cfg.jfc = jfc_;
        jetty_cfg.flag.bs.share_jfr = 1;
        jetty_cfg.shared.jfr = jfr_;
        jetty_cfg.shared.jfc = jfc_;
        jetty_ = urma_create_jetty(ctx_, &jetty_cfg);
        if (jetty_ == nullptr) throw std::runtime_error("urma_create_jetty failed");

        local_ep_.protocol_version = PROTOCOL_VERSION;
        local_ep_.uasid = jetty_->jetty_id.uasid;
        local_ep_.jetty_id = jetty_->jetty_id.id;
        local_ep_.max_chunk_bytes = local_max_raw_operation_bytes_;
        local_ep_.transport_mode = URMA_TM_RC;
        std::memcpy(local_ep_.eid, &ctx_->eid, sizeof(local_ep_.eid));
        std::fprintf(stderr, "SPARK_URMA_INIT eid=%02x%02x%02x%02x... jetty_uasid=%u jetty_id=%u ctx_eid_index=%u\n",
                     local_ep_.eid[0], local_ep_.eid[1], local_ep_.eid[2], local_ep_.eid[3],
                     local_ep_.uasid, local_ep_.jetty_id, ctx_->eid_index);
        state_ = TransportState::INITIALIZED;
    } catch (const std::exception& original) {
        std::vector<std::string> errors;
        cleanup_locked(errors);
        throw std::runtime_error(std::string("ERR_INIT: ") + original.what() +
                                 (errors.empty() ? "" : "; cleanup: " + join_errors(errors)));
    }
}

void UrmaTransport::shutdown_process_runtime() {
    std::lock_guard<std::mutex> process_lock(g_process_mutex);
    if (g_transport_active) {
        throw std::runtime_error("ERR_BUSY: cannot shutdown process runtime with an active transport");
    }
    if (!g_runtime_initialized) return;
    if (urma_uninit() != URMA_SUCCESS) {
        throw std::runtime_error("ERR_CLOSED: urma_uninit failed");
    }
    g_runtime_initialized = false;
    g_runtime_shutdown = true;
    ++g_runtime_uninit_calls;
}

ProcessRuntimeMetrics UrmaTransport::process_runtime_metrics() {
    std::lock_guard<std::mutex> process_lock(g_process_mutex);
    ProcessRuntimeMetrics metrics;
    metrics.generation = g_transport_epoch;
    metrics.init_calls = g_runtime_init_calls;
    metrics.uninit_calls = g_runtime_uninit_calls;
    metrics.active_transports = g_transport_active ? 1 : 0;
    metrics.initialized = g_runtime_initialized;
    return metrics;
}

void UrmaTransport::connect(const EndpointDescriptor& remote) {
    LOCK();
    if (state_ == TransportState::CONNECTED) {
        if (metrics_.inflight_requests.load() != 0 ||
            metrics_.provider_outstanding_requests.load() != 0) {
            throw std::runtime_error("ERR_BUSY: cannot reconnect with provider requests in flight");
        }
        for (auto& imported : imported_regions_) {
            if (imported.segment != nullptr && urma_unimport_seg(imported.segment) != URMA_SUCCESS) {
                throw std::runtime_error("ERR_CONNECTION: failed to unimport old remote segment");
            }
            if (imported.segment != nullptr) {
                metrics_.unimported_regions.fetch_add(1);
                metrics_.active_imports.fetch_sub(1);
            }
        }
        imported_regions_.clear();
        if (remote_jetty_ != nullptr) {
            if (urma_unbind_jetty(jetty_) != URMA_SUCCESS) {
                throw std::runtime_error("ERR_CONNECTION: failed to unbind old peer");
            }
            if (urma_unimport_jetty(remote_jetty_) != URMA_SUCCESS) {
                throw std::runtime_error("ERR_CONNECTION: failed to unimport old peer");
            }
            remote_jetty_ = nullptr;
        }
        state_ = TransportState::INITIALIZED;
    } else if (state_ != TransportState::INITIALIZED) {
        throw std::runtime_error("ERR_CONNECTION: transport is not initialized");
    }
    if (remote.protocol_version != PROTOCOL_VERSION || remote.max_chunk_bytes == 0) {
        throw std::runtime_error("ERR_PROTOCOL: incompatible endpoint descriptor");
    }
    remote_ep_ = remote;
    effective_max_raw_operation_bytes_ =
        std::min<uint32_t>(
            local_max_raw_operation_bytes_, remote.max_chunk_bytes);
    if (effective_max_raw_operation_bytes_ == 0) {
        throw std::runtime_error(
            "ERR_PROTOCOL: negotiated raw operation limit is zero");
    }
    std::fprintf(stderr,
        "SPARK_URMA_NEGOTIATED localMax=%u peerMax=%u effectiveMax=%u\n",
        local_max_raw_operation_bytes_, remote.max_chunk_bytes,
        effective_max_raw_operation_bytes_);
    std::fprintf(stderr, "SPARK_URMA_CONNECT seg_uasid=%u seg_gen=%llu token=%u addr=%#llx\n", remote.segment_uasid, (unsigned long long)remote.segment_generation, remote.segment_token, (unsigned long long)remote.segment_address);
    urma_rjetty_t remote_jetty = {};
    std::memcpy(&remote_jetty.jetty_id.eid, remote.eid, sizeof(remote.eid));
    remote_jetty.jetty_id.uasid = remote.uasid;
    remote_jetty.jetty_id.id = remote.jetty_id;
    remote_jetty.trans_mode = URMA_TM_RC;
    urma_token_t token = {.token = 0xDEADBEEF};
    remote_jetty_ = urma_import_jetty(ctx_, &remote_jetty, &token);
    if (remote_jetty_ == nullptr) throw std::runtime_error("ERR_CONNECTION: urma_import_jetty failed");
    if (urma_bind_jetty(jetty_, remote_jetty_) != URMA_SUCCESS) {
        (void)urma_unimport_jetty(remote_jetty_);
        remote_jetty_ = nullptr;
        throw std::runtime_error("ERR_CONNECTION: urma_bind_jetty failed");
    }
    state_ = TransportState::CONNECTED;
}

RegisteredRegion UrmaTransport::register_region(void* address, size_t length) {
    LOCK();
    if (state_ != TransportState::INITIALIZED && state_ != TransportState::CONNECTED) {
        throw std::runtime_error("ERR_CLOSED: registration requires an open transport");
    }
    if (address == nullptr || length == 0) {
        throw std::runtime_error("ERR_INVALID_ARG: registration address/length is invalid");
    }
    const uint64_t base = reinterpret_cast<uint64_t>(address);
    if (base > std::numeric_limits<uint64_t>::max() - length) {
        throw std::runtime_error("ERR_BOUNDS: registration range overflow");
    }
    for (const auto& item : registered_regions_) {
        const RegisteredRegion& existing = item.second.region;
        if (range_contains(existing.remote_address, existing.length, base, length) ||
            range_contains(base, length, existing.remote_address, existing.length)) {
            throw std::runtime_error("ERR_REGISTRATION: overlapping registration");
        }
    }

    urma_token_id_t* token_id = urma_alloc_token_id(ctx_);
    if (token_id == nullptr) {
        throw std::runtime_error("ERR_REGISTRATION: urma_alloc_token_id failed");
    }
    urma_seg_cfg_t cfg = {};
    cfg.va = base;
    cfg.len = length;
    cfg.token_id = token_id;
    cfg.token_value.token = 0xDEADBEEF;
    cfg.flag.bs.token_id_valid = URMA_TOKEN_ID_VALID;
    cfg.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE;
    urma_target_seg_t* segment = urma_register_seg(ctx_, &cfg);
    if (segment == nullptr) {
        (void)urma_free_token_id(token_id);
        throw std::runtime_error("ERR_REGISTRATION: urma_register_seg failed");
    }

    RegisteredRegion region;
    region.handle = next_region_handle_++;
    region.address = address;
    region.length = length;
    region.remote_address = cfg.va;
    region.token = segment->seg.token_id;
    // The Q8 native-vdev provider uses the fourth urma_import_seg argument as
    // the registered-MR generation.  It is intentionally not part of the
    // public urma_seg_t, so consume the provider's diagnostic ABI when it is
    // available.  Other providers keep generation=0 and retain the public
    // NOMAP import semantics.
#if defined(SPARK_URMA_HAS_VDEV_MR_INFO)
    spark_openurma_vdev_mr_resp mr_info = {};
    const auto mr_info_fn = resolve_vdev_mr_info();
    bool vdev_mr_info_valid = false;
    if (mr_info_fn != nullptr && mr_info_fn(segment, &mr_info) == 0) {
        vdev_mr_info_valid = true;
        region.generation = mr_info.generation;
        std::fprintf(stderr, "SPARK_URMA_MR ctx_eid=%02x%02x%02x%02x seg_eid=%02x%02x%02x%02x eid_index=%u token=%u gen=%llu\n",
                     ((unsigned char*)&ctx_->eid)[0], ((unsigned char*)&ctx_->eid)[1], ((unsigned char*)&ctx_->eid)[2], ((unsigned char*)&ctx_->eid)[3],
                     ((unsigned char*)&segment->seg.ubva.eid)[0], ((unsigned char*)&segment->seg.ubva.eid)[1], ((unsigned char*)&segment->seg.ubva.eid)[2], ((unsigned char*)&segment->seg.ubva.eid)[3], mr_info.eid_index, mr_info.token_id, (unsigned long long)mr_info.generation);
        // The Q8 provider's public urma_seg_t leaves token_id unset while
        // retaining the provider-issued token in its diagnostic MR record.
        // Exchange that real token, plus the provider canonical VA/length,
        // so urma_import_seg can validate the peer MR.
        if (mr_info.token_id != 0) region.token = mr_info.token_id;
        if (mr_info.va != 0) region.remote_address = mr_info.va;
        if (mr_info.len != 0) region.length = mr_info.len;
        local_ep_.segment_uasid = mr_info.eid_index;
        local_ep_.segment_generation = mr_info.generation;
    }
#endif
#if defined(SPARK_URMA_HAS_VDEV_MR_INFO)
    // Q8 uses eid_index=0 as a valid device-local segment identity.  Do not
    // treat zero as "missing" and replace it with the public ubva.uasid
    // (typically 4096); that value is not accepted by the provider's MR
    // lookup and makes every remote READ complete with REM_ACCESS_ABORT.
    if (!vdev_mr_info_valid && local_ep_.segment_uasid == 0)
        local_ep_.segment_uasid = segment->seg.ubva.uasid;
#else
    if (local_ep_.segment_uasid == 0) local_ep_.segment_uasid = segment->seg.ubva.uasid;
#endif
    region.owner_epoch = owner_epoch_;
    LocalRegionState state;
    state.region = region;
    state.segment = segment;
    state.token_id = token_id;
    registered_regions_.emplace(region.handle, state);
    metrics_.registered_regions.fetch_add(1);
    metrics_.active_regions.fetch_add(1);

    // Endpoint identity and region identity are logically separate. These fields are only
    // the diagnostic default region; each operation imports its supplied RemoteRegion.
    local_ep_.segment_address = region.remote_address;
    local_ep_.segment_length = region.length;
    local_ep_.segment_token = static_cast<uint32_t>(region.token);
    return region;
}

RegisteredRegion UrmaTransport::registered_region(uint64_t handle) const {
    LOCK();
    auto it = registered_regions_.find(handle);
    if (it == registered_regions_.end() || !it->second.registered) {
        throw std::runtime_error("ERR_REGISTRATION: unknown or unregistered handle");
    }
    return it->second.region;
}

void UrmaTransport::unregister_region(uint64_t handle) {
    LOCK();
    auto it = registered_regions_.find(handle);
    if (it == registered_regions_.end() || !it->second.registered) {
        throw std::runtime_error("ERR_REGISTRATION: unknown or already unregistered handle");
    }
    if (it->second.inflight != 0) {
        throw std::runtime_error("ERR_BUSY: region has in-flight provider requests");
    }
    if (urma_unregister_seg(it->second.segment) != URMA_SUCCESS) {
        throw std::runtime_error("ERR_REGISTRATION: urma_unregister_seg failed");
    }
    if (it->second.token_id != nullptr && urma_free_token_id(it->second.token_id) != URMA_SUCCESS) {
        throw std::runtime_error("ERR_REGISTRATION: urma_free_token_id failed");
    }
    it->second.registered = false;
    it->second.segment = nullptr;
    it->second.token_id = nullptr;
    metrics_.unregistered_regions.fetch_add(1);
    metrics_.active_regions.fetch_sub(1);
    registered_regions_.erase(it);
}

void UrmaTransport::release_cached_remote_imports() {
    LOCK();
    // A provider CQE can arrive after the API-level request has become terminal.
    // Drain those late completions before unimporting peer segments; otherwise cleanup
    // races the provider and returns ERR_BUSY even though transport inflight is zero.
    const auto drain_deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(std::min<uint32_t>(std::max<uint32_t>(1, options_.request_timeout_ms), 5000));
    while (metrics_.provider_outstanding_requests.load() != 0 &&
           std::chrono::steady_clock::now() < drain_deadline) {
        poll_once_locked();
        if (metrics_.provider_outstanding_requests.load() != 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    }
    if (metrics_.provider_outstanding_requests.load() != 0) {
        throw std::runtime_error(
            "ERR_BUSY: provider requests are outstanding while releasing remote imports");
    }
    for (const auto& imported : imported_regions_) {
        if (imported.inflight != 0) {
            throw std::runtime_error(
                "ERR_BUSY: remote import has in-flight provider references");
        }
    }
    for (auto it = imported_regions_.begin(); it != imported_regions_.end();) {
        if (it->segment != nullptr && urma_unimport_seg(it->segment) != URMA_SUCCESS) {
            throw std::runtime_error(
                "ERR_CONNECTION: failed to release cached remote segment import");
        }
        if (it->segment != nullptr) {
            metrics_.unimported_regions.fetch_add(1);
            metrics_.active_imports.fetch_sub(1);
        }
        it = imported_regions_.erase(it);
    }
}

UrmaTransport::LocalRegionState& UrmaTransport::find_local_region_locked(
    const void* address, size_t length) {
    const uint64_t addr = reinterpret_cast<uint64_t>(address);
    for (auto& item : registered_regions_) {
        auto& state = item.second;
        if (state.registered && range_contains(state.region.remote_address,
                                               state.region.length, addr, length)) {
            return state;
        }
    }
    throw std::runtime_error("ERR_REGISTRATION: local range is not registered by this transport");
}

UrmaTransport::ImportedRegionState& UrmaTransport::find_or_import_remote_locked(
    const RemoteRegion& remote) {
    if (remote.remote_address == 0 || remote.length == 0 || remote.token > UINT32_MAX) {
        throw std::runtime_error("ERR_BOUNDS: invalid remote region descriptor");
    }
    for (auto& imported : imported_regions_) {
        if (imported.region.remote_address == remote.remote_address &&
            imported.region.length == remote.length && imported.region.token == remote.token) {
            imported.last_used = ++import_clock_;
            return imported;
        }
    }
    if (imported_regions_.size() >= MAX_REMOTE_IMPORTS) {
        auto victim = imported_regions_.end();
        for (auto it = imported_regions_.begin(); it != imported_regions_.end(); ++it) {
            if (it->inflight == 0 &&
                (victim == imported_regions_.end() || it->last_used < victim->last_used)) {
                victim = it;
            }
        }
        if (victim == imported_regions_.end()) {
            throw std::runtime_error("ERR_BUSY: remote import cache is full with in-flight regions");
        }
        if (urma_unimport_seg(victim->segment) != URMA_SUCCESS) {
            throw std::runtime_error("ERR_CONNECTION: failed to evict remote segment import");
        }
        metrics_.unimported_regions.fetch_add(1);
        metrics_.active_imports.fetch_sub(1);
        imported_regions_.erase(victim);
    }
    urma_seg_t segment = {};
    std::memcpy(&segment.ubva.eid, remote_ep_.eid, sizeof(remote_ep_.eid));
    segment.ubva.uasid = remote_ep_.segment_uasid;
    segment.ubva.va = remote.remote_address;
    segment.len = remote.length;
    segment.token_id = static_cast<uint32_t>(remote.token);
    urma_token_t token = {.token = 0xDEADBEEF};
    // The per-arena provider MR generation travels with RemoteBuffer.  The
    // endpoint generation is only a bootstrap/default and is not sufficient
    // when a peer has more than one registered arena.
    urma_target_seg_t* imported = urma_import_seg(
        ctx_, &segment, &token, remote.generation, urma_import_seg_flag_t{});
    std::fprintf(stderr, "SPARK_URMA_IMPORT remote_seg_uasid=%u remote_eid=%02x%02x%02x%02x... addr=%#llx token=%llu seggen=%llu\n",
                 remote_ep_.segment_uasid, remote_ep_.eid[0], remote_ep_.eid[1], remote_ep_.eid[2], remote_ep_.eid[3],
                 (unsigned long long)remote.remote_address, (unsigned long long)remote.token, (unsigned long long)remote.generation);
    if (imported == nullptr) throw std::runtime_error("ERR_CONNECTION: urma_import_seg failed");
    imported_regions_.push_back({remote, imported, 0, ++import_clock_});
    metrics_.imported_regions.fetch_add(1);
    metrics_.active_imports.fetch_add(1);
    return imported_regions_.back();
}

uint64_t UrmaTransport::post_rw_locked(bool write_op, const RemoteRegion& remote,
                                       void* local, size_t length, size_t remote_offset) {
    if (state_ != TransportState::CONNECTED) throw std::runtime_error("ERR_CONNECTION: not connected");
    if (length == 0 ||
        length > effective_max_raw_operation_bytes_ ||
        length > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("ERR_INVALID_ARG: raw request length must be 1..maxChunkBytes");
    }
    if (remote_offset > remote.length || length > remote.length - remote_offset) {
        throw std::runtime_error("ERR_BOUNDS: remote offset/length exceeds descriptor");
    }
    if (remote.remote_address > UINT64_MAX - remote_offset) {
        throw std::runtime_error("ERR_BOUNDS: remote address overflow");
    }
    LocalRegionState& local_region = find_local_region_locked(local, length);
    ImportedRegionState& imported = find_or_import_remote_locked(remote);
    const uint64_t request_id = next_request_id();

    urma_sge_t local_sge = {};
    local_sge.addr = reinterpret_cast<uint64_t>(local);
    local_sge.len = static_cast<uint32_t>(length);
    local_sge.tseg = local_region.segment;
    urma_sge_t remote_sge = {};
    remote_sge.addr = remote.remote_address + remote_offset;
    remote_sge.len = static_cast<uint32_t>(length);
    remote_sge.tseg = imported.segment;
    urma_sg_t source = {.sge = write_op ? &local_sge : &remote_sge, .num_sge = 1};
    urma_sg_t destination = {.sge = write_op ? &remote_sge : &local_sge, .num_sge = 1};
    urma_jfs_wr_t wr = {};
    wr.flag.bs.complete_enable = 1;
    wr.opcode = write_op ? URMA_OPC_WRITE : URMA_OPC_READ;
    wr.tjetty = remote_jetty_;
    wr.user_ctx = request_id;
    wr.rw.src = source;
    wr.rw.dst = destination;
    urma_jfs_wr_t* bad = nullptr;
    urma_status_t post_status = URMA_EAGAIN;
    const auto post_deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(std::max<uint32_t>(1, options_.request_timeout_ms));
    for (uint64_t spins = 0; ; ++spins) {
        bad = nullptr;
        post_status = urma_post_jfs_wr(jfs_, &wr, &bad);
        if (post_status == URMA_SUCCESS) break;
        if (post_status != URMA_EAGAIN) break;
        // Q8 exposes a finite provider ring. Drain completions while the
        // logical block fan-out is being posted.  A fixed spin count made a
        // transiently full ring surface as a false READ/WRITE post failure
        // under the normal Spark fetch concurrency; use the transport
        // request deadline and yield periodically instead.
        poll_once_locked();
        if (std::chrono::steady_clock::now() >= post_deadline) break;
        if ((spins & 0x3ffu) == 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        } else {
            std::this_thread::yield();
        }
    }
    if (post_status != URMA_SUCCESS) {
        std::fprintf(stderr, "SPARK_URMA_POST_FAIL op=%s status=%d bad=%p opcode=%d flag=%#x\n",
                     write_op ? "WRITE" : "READ", static_cast<int>(post_status),
                     static_cast<void*>(bad), wr.opcode, wr.flag.value);
        throw std::runtime_error(write_op ? "ERR_COMPLETION: WRITE post failed"
                                          : "ERR_COMPLETION: READ post failed");
    }

    RequestRecord request;
    request.request_id = request_id;
    request.is_write = write_op;
    request.is_read = !write_op;
    request.bytes = length;
    request.state = RequestState::POSTED;
    request.local_region_handle = local_region.region.handle;
    request.remote_segment = imported.segment;
    request.provider_outstanding = true;
    requests_.emplace(request_id, request);
    ++local_region.inflight;
    ++imported.inflight;
    metrics_.record_provider_post(request_id);
    if (write_op) metrics_.record_write(request_id, length);
    else metrics_.record_read(request_id, length);
    return request_id;
}

uint64_t UrmaTransport::write(const RemoteRegion& remote, const void* source,
                              size_t length, size_t remote_offset) {
    LOCK();
    return post_rw_locked(true, remote, const_cast<void*>(source), length, remote_offset);
}

uint64_t UrmaTransport::read(const RemoteRegion& remote, void* destination,
                             size_t length, size_t remote_offset) {
    LOCK();
    return post_rw_locked(false, remote, destination, length, remote_offset);
}

uint64_t UrmaTransport::send(const void* message, size_t length) {
    LOCK();
    if (state_ != TransportState::CONNECTED) throw std::runtime_error("ERR_CONNECTION: not connected");
    if (length == 0 ||
        length > effective_max_raw_operation_bytes_ ||
        length > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("ERR_INVALID_ARG: SEND length must be 1..maxChunkBytes");
    }
    LocalRegionState& local_region = find_local_region_locked(message, length);
    const uint64_t request_id = next_request_id();
    urma_sge_t sge = {};
    sge.addr = reinterpret_cast<uint64_t>(const_cast<void*>(message));
    sge.len = static_cast<uint32_t>(length);
    sge.tseg = local_region.segment;
    urma_jfs_wr_t wr = {};
    wr.opcode = URMA_OPC_SEND;
    wr.tjetty = remote_jetty_;
    wr.user_ctx = request_id;
    wr.send.src.sge = &sge;
    wr.send.src.num_sge = 1;
    urma_jfs_wr_t* bad = nullptr;
    urma_status_t post_status = URMA_EAGAIN;
    const auto post_deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(std::max<uint32_t>(1, options_.request_timeout_ms));
    for (uint64_t spins = 0; ; ++spins) {
        bad = nullptr;
        post_status = urma_post_jfs_wr(jfs_, &wr, &bad);
        if (post_status == URMA_SUCCESS) break;
        if (post_status != URMA_EAGAIN) break;
        poll_once_locked();
        if (std::chrono::steady_clock::now() >= post_deadline) break;
        if ((spins & 0x3ffu) == 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        } else {
            std::this_thread::yield();
        }
    }
    if (post_status != URMA_SUCCESS) {
        std::fprintf(stderr, "SPARK_URMA_POST_FAIL op=SEND status=%d bad=%p opcode=%d flag=%#x\n",
                     static_cast<int>(post_status), static_cast<void*>(bad), wr.opcode, wr.flag.value);
        throw std::runtime_error("ERR_COMPLETION: SEND post failed");
    }
    RequestRecord request;
    request.request_id = request_id;
    request.is_send = true;
    request.bytes = length;
    request.state = RequestState::POSTED;
    request.local_region_handle = local_region.region.handle;
    request.provider_outstanding = true;
    requests_.emplace(request_id, request);
    ++local_region.inflight;
    metrics_.record_provider_post(request_id);
    metrics_.record_send(request_id, length);
    return request_id;
}

uint64_t UrmaTransport::writeBlock(const RemoteRegion& remote, const void* source,
                                   size_t total_length) {
    const uint64_t aggregate_id = next_request_id();
    {
        LOCK();
        RequestRecord aggregate;
        aggregate.request_id = aggregate_id;
        aggregate.is_aggregate = true;
        aggregate.aggregate_building = true;
        aggregate.bytes = total_length;
        aggregate.state = RequestState::POSTED;
        requests_.emplace(aggregate_id, aggregate);
        metrics_.logical_blocks.fetch_add(1);
        if (total_length == 0) {
            auto& stored = requests_.at(aggregate_id);
            stored.aggregate_building = false;
            finalize_locked(stored, RequestState::COMPLETED_SUCCESS, 0, 0, "zero-byte logical block");
            return aggregate_id;
        }
    }
    const auto* bytes = static_cast<const uint8_t*>(source);
    try {
        const size_t raw_limit = effective_max_raw_operation_bytes_;
        for (size_t offset = 0; offset < total_length; offset += raw_limit) {
            const size_t chunk =
                std::min<size_t>(raw_limit, total_length - offset);
            const uint64_t child = write(remote, bytes + offset, chunk, offset);
            LOCK();
            requests_.at(aggregate_id).children.push_back(child);
            auto child_it = requests_.find(child);
            if (child_it != requests_.end()) child_it->second.aggregate_parent = aggregate_id;
            metrics_.chunks.fetch_add(1);
        }
        metrics_.chunked_blocks.fetch_add(1);
        LOCK();
        requests_.at(aggregate_id).aggregate_building = false;
    } catch (const std::exception& error) {
        LOCK();
        auto& aggregate = requests_.at(aggregate_id);
        aggregate.aggregate_building = false;
        finalize_locked(aggregate, RequestState::COMPLETED_FAILED,
                        static_cast<int>(ErrorCode::ERR_COMPLETION), 0, error.what());
    }
    return aggregate_id;
}

uint64_t UrmaTransport::readBlock(const RemoteRegion& remote, void* destination,
                                  size_t total_length) {
    const uint64_t aggregate_id = next_request_id();
    {
        LOCK();
        RequestRecord aggregate;
        aggregate.request_id = aggregate_id;
        aggregate.is_aggregate = true;
        aggregate.aggregate_building = true;
        aggregate.bytes = total_length;
        aggregate.state = RequestState::POSTED;
        requests_.emplace(aggregate_id, aggregate);
        metrics_.logical_blocks.fetch_add(1);
        if (total_length == 0) {
            auto& stored = requests_.at(aggregate_id);
            stored.aggregate_building = false;
            finalize_locked(stored, RequestState::COMPLETED_SUCCESS, 0, 0, "zero-byte logical block");
            return aggregate_id;
        }
    }
    auto* bytes = static_cast<uint8_t*>(destination);
    try {
        const size_t raw_limit = effective_max_raw_operation_bytes_;
        for (size_t offset = 0; offset < total_length; offset += raw_limit) {
            const size_t chunk =
                std::min<size_t>(raw_limit, total_length - offset);
            const uint64_t child = read(remote, bytes + offset, chunk, offset);
            LOCK();
            requests_.at(aggregate_id).children.push_back(child);
            auto child_it = requests_.find(child);
            if (child_it != requests_.end()) child_it->second.aggregate_parent = aggregate_id;
            metrics_.chunks.fetch_add(1);
        }
        metrics_.chunked_blocks.fetch_add(1);
        LOCK();
        requests_.at(aggregate_id).aggregate_building = false;
    } catch (const std::exception& error) {
        LOCK();
        auto& aggregate = requests_.at(aggregate_id);
        aggregate.aggregate_building = false;
        finalize_locked(aggregate, RequestState::COMPLETED_FAILED,
                        static_cast<int>(ErrorCode::ERR_COMPLETION), 0, error.what());
    }
    return aggregate_id;
}

uint64_t UrmaTransport::write_chunked(const RemoteRegion& remote, const void* source,
                                      size_t total_length) {
    return writeBlock(remote, source, total_length);
}

uint64_t UrmaTransport::read_chunked(const RemoteRegion& remote, void* destination,
                                     size_t total_length) {
    return readBlock(remote, destination, total_length);
}

void UrmaTransport::finalize_locked(RequestRecord& request, RequestState state, int status,
                                    size_t completed_bytes, const std::string& message) {
    if (request.state != RequestState::POSTED && request.state != RequestState::CREATED) return;
    request.state = state;
    request.status = status;
    request.completed_bytes = completed_bytes;
    request.message = message;
    if (request.is_aggregate) {
        // The child records are protected while this aggregate is POSTED;
        // release that protection only after the aggregate itself becomes
        // terminal so they can be reclaimed by the bounded cache.
        for (const uint64_t child_id : request.children) {
            auto child = requests_.find(child_id);
            if (child != requests_.end() && child->second.aggregate_parent == request.request_id) {
                child->second.aggregate_parent = 0;
            }
        }
    }
    if (!request.is_aggregate) {
        if (state == RequestState::COMPLETED_SUCCESS) {
            metrics_.record_completion(request.request_id, true);
            metrics_.completed_bytes.fetch_add(completed_bytes);
        }
        else if (state == RequestState::TIMED_OUT_PROVIDER_OUTSTANDING ||
                 state == RequestState::TIMED_OUT_DRAINED) {
            metrics_.record_timeout(request.request_id);
        }
        else metrics_.record_completion(request.request_id, false);
        if (!request.provider_outstanding && !request.local_ref_released) {
            auto region = registered_regions_.find(request.local_region_handle);
            if (region != registered_regions_.end() && region->second.inflight != 0) --region->second.inflight;
            request.local_ref_released = true;
        }
    }
    terminal_order_.push_back(request.request_id);
    trim_terminal_cache_locked();
}

void UrmaTransport::provider_drained_locked(RequestRecord& request) {
    if (!request.provider_outstanding) return;
    request.provider_outstanding = false;
    metrics_.record_provider_drained(request.request_id);
    if (!request.local_ref_released) {
        auto region = registered_regions_.find(request.local_region_handle);
        if (region != registered_regions_.end() && region->second.inflight != 0) {
            --region->second.inflight;
        }
        request.local_ref_released = true;
    }
    if (request.remote_segment != nullptr) {
        for (auto& imported : imported_regions_) {
            if (imported.segment == request.remote_segment) {
                if (imported.inflight != 0) --imported.inflight;
                break;
            }
        }
        request.remote_segment = nullptr;
    }
    if (request.state == RequestState::TIMED_OUT_PROVIDER_OUTSTANDING) {
        request.state = RequestState::TIMED_OUT_DRAINED;
    } else if (request.state == RequestState::CANCELLED_PROVIDER_OUTSTANDING) {
        request.state = RequestState::CANCELLED_DRAINED;
    }
}

void UrmaTransport::poll_once_locked() {
    urma_cr_t completion = {};
    const int count = urma_poll_jfc(jfc_, 1, &completion);
    if (count < 0) throw std::runtime_error("ERR_COMPLETION: urma_poll_jfc failed");
    if (count == 0) return;
    auto it = requests_.find(completion.user_ctx);
    if (it == requests_.end() || it->second.is_aggregate) {
        metrics_.late_completions.fetch_add(1);
        return;
    }
    RequestRecord& request = it->second;
    if (request.state == RequestState::TIMED_OUT_PROVIDER_OUTSTANDING ||
        request.state == RequestState::CANCELLED_PROVIDER_OUTSTANDING) {
        metrics_.late_completions.fetch_add(1);
        provider_drained_locked(request);
        return;
    }
    if (request.state != RequestState::POSTED) {
        metrics_.late_completions.fetch_add(1);
        return;
    }
    const bool success = completion.status == URMA_CR_SUCCESS;
    provider_drained_locked(request);
    finalize_locked(request,
                    success ? RequestState::COMPLETED_SUCCESS : RequestState::COMPLETED_FAILED,
                    success ? 0 : static_cast<int>(ErrorCode::ERR_COMPLETION),
                    success ? request.bytes : 0,
                    success ? "" : "provider completion failure status=" + std::to_string(completion.status));
}

void UrmaTransport::refresh_aggregates_locked() {
    std::vector<uint64_t> aggregate_ids;
    aggregate_ids.reserve(requests_.size());
    for (const auto& item : requests_) {
        if (item.second.is_aggregate && item.second.state == RequestState::POSTED) {
            aggregate_ids.push_back(item.first);
        }
    }
    for (uint64_t aggregate_id : aggregate_ids) {
        auto aggregate_it = requests_.find(aggregate_id);
        if (aggregate_it == requests_.end()) continue;
        RequestRecord& aggregate = aggregate_it->second;
        if (!aggregate.is_aggregate || aggregate.state != RequestState::POSTED ||
            aggregate.aggregate_building) continue;
        bool all_success = !aggregate.children.empty();
        size_t completed = 0;
        for (uint64_t child_id : aggregate.children) {
            auto child_it = requests_.find(child_id);
            if (child_it == requests_.end()) {
                finalize_locked(aggregate, RequestState::COMPLETED_FAILED,
                                static_cast<int>(ErrorCode::ERR_INTERNAL), completed,
                                "aggregate child was evicted unexpectedly");
                break;
            }
            const RequestRecord& child = child_it->second;
            if (child.state == RequestState::COMPLETED_FAILED ||
                child.state == RequestState::TIMED_OUT_PROVIDER_OUTSTANDING ||
                child.state == RequestState::TIMED_OUT_DRAINED ||
                child.state == RequestState::CANCELLED_PROVIDER_OUTSTANDING ||
                child.state == RequestState::CANCELLED_DRAINED) {
                finalize_locked(aggregate, RequestState::COMPLETED_FAILED, child.status,
                                completed, "child " + std::to_string(child_id) + ": " + child.message);
                break;
            }
            if (child.state != RequestState::COMPLETED_SUCCESS) all_success = false;
            else completed += child.completed_bytes;
        }
        if (aggregate.state == RequestState::POSTED && all_success) {
            if (completed == aggregate.bytes) {
                finalize_locked(aggregate, RequestState::COMPLETED_SUCCESS, 0, completed, "");
            } else {
                finalize_locked(aggregate, RequestState::COMPLETED_FAILED,
                                static_cast<int>(ErrorCode::ERR_INTERNAL), completed,
                                "aggregate completed byte count mismatch");
            }
        }
    }
}

CompletionResult UrmaTransport::result_locked(const RequestRecord& request) const {
    return CompletionResult{request.request_id, request.status,
                            request.completed_bytes, request.message};
}

void UrmaTransport::trim_terminal_cache_locked() {
    size_t examined = 0;
    while (terminal_order_.size() > MAX_TERMINAL_RESULTS && examined < terminal_order_.size()) {
        uint64_t id = terminal_order_.front();
        terminal_order_.pop_front();
        auto it = requests_.find(id);
        if (it != requests_.end() && it->second.state != RequestState::POSTED &&
            !it->second.provider_outstanding && it->second.aggregate_parent == 0) {
            requests_.erase(it);
        } else {
            terminal_order_.push_back(id);
            ++examined;
        }
    }
}

CompletionResult UrmaTransport::wait(uint64_t request_id, uint32_t timeout_ms) {
    if (request_id == 0 || timeout_ms == 0) {
        throw std::runtime_error("ERR_INVALID_ARG: request id and timeout must be positive");
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (true) {
        {
            LOCK();
            auto it = requests_.find(request_id);
            if (it == requests_.end()) {
                metrics_.unknown_waits.fetch_add(1);
                throw std::runtime_error("ERR_UNKNOWN_REQUEST: request id is not known");
            }
            refresh_aggregates_locked();
            if (it->second.state != RequestState::POSTED && it->second.state != RequestState::CREATED) {
                return result_locked(it->second);
            }
            poll_once_locked();
            refresh_aggregates_locked();
            it = requests_.find(request_id);
            if (it != requests_.end() && it->second.state != RequestState::POSTED) {
                return result_locked(it->second);
            }
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            LOCK();
            auto it = requests_.find(request_id);
            if (it == requests_.end()) {
                metrics_.unknown_waits.fetch_add(1);
                throw std::runtime_error("ERR_UNKNOWN_REQUEST: request disappeared");
            }
            if (it->second.state == RequestState::POSTED) {
                const RequestState timeout_state = it->second.provider_outstanding
                    ? RequestState::TIMED_OUT_PROVIDER_OUTSTANDING
                    : RequestState::TIMED_OUT_DRAINED;
                finalize_locked(it->second, timeout_state,
                                static_cast<int>(ErrorCode::ERR_TIMEOUT), 0,
                                "request deadline exceeded");
            }
            return result_locked(it->second);
        }
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
}

void UrmaTransport::cleanup_locked(std::vector<std::string>& errors) {
    if (metrics_.provider_outstanding_requests.load() != 0) {
        errors.push_back("provider requests are still outstanding");
        return;
    }
    for (const auto& item : registered_regions_) {
        if (item.second.inflight != 0) {
            errors.push_back("registered region still has provider references: " +
                             std::to_string(item.first));
        }
    }
    if (!errors.empty()) return;

    for (auto& imported : imported_regions_) {
        if (imported.segment != nullptr && urma_unimport_seg(imported.segment) != URMA_SUCCESS)
            errors.push_back("urma_unimport_seg failed");
        else if (imported.segment != nullptr) {
            metrics_.unimported_regions.fetch_add(1);
            metrics_.active_imports.fetch_sub(1);
        }
    }
    imported_regions_.clear();
    if (remote_jetty_ != nullptr) {
        if (jetty_ != nullptr && urma_unbind_jetty(jetty_) != URMA_SUCCESS)
            errors.push_back("urma_unbind_jetty failed");
        if (urma_unimport_jetty(remote_jetty_) != URMA_SUCCESS)
            errors.push_back("urma_unimport_jetty failed");
        remote_jetty_ = nullptr;
    }
    for (auto& item : registered_regions_) {
        if (item.second.segment != nullptr && urma_unregister_seg(item.second.segment) != URMA_SUCCESS)
            errors.push_back("urma_unregister_seg failed for handle " + std::to_string(item.first));
        else if (item.second.segment != nullptr) {
            metrics_.unregistered_regions.fetch_add(1);
            metrics_.active_regions.fetch_sub(1);
        }
        if (item.second.token_id != nullptr && urma_free_token_id(item.second.token_id) != URMA_SUCCESS)
            errors.push_back("urma_free_token_id failed for handle " + std::to_string(item.first));
        item.second.token_id = nullptr;
    }
    registered_regions_.clear();
    if (local_seg_ != nullptr) {
        if (urma_unregister_seg(local_seg_) != URMA_SUCCESS) errors.push_back("internal unregister failed");
        local_seg_ = nullptr;
    }
    std::free(local_buffer_);
    local_buffer_ = nullptr;
    if (jetty_ != nullptr && urma_delete_jetty(jetty_) != URMA_SUCCESS) errors.push_back("urma_delete_jetty failed");
    jetty_ = nullptr;
    if (jfs_ != nullptr && urma_delete_jfs(jfs_) != URMA_SUCCESS) errors.push_back("urma_delete_jfs failed");
    jfs_ = nullptr;
    if (jfr_ != nullptr && urma_delete_jfr(jfr_) != URMA_SUCCESS) errors.push_back("urma_delete_jfr failed");
    jfr_ = nullptr;
    if (jfc_ != nullptr && urma_delete_jfc(jfc_) != URMA_SUCCESS) errors.push_back("urma_delete_jfc failed");
    jfc_ = nullptr;
    if (ctx_ != nullptr && urma_delete_context(ctx_) != URMA_SUCCESS) errors.push_back("urma_delete_context failed");
    ctx_ = nullptr;
    if (owns_runtime_slot_) {
        std::lock_guard<std::mutex> process_lock(g_process_mutex);
        g_transport_active = false;
        owns_runtime_slot_ = false;
        if (metrics_.active_transports.load() != 0) metrics_.active_transports.fetch_sub(1);
    }
}

void UrmaTransport::close() {
    LOCK();
    if (state_ == TransportState::CLOSED) return;
    state_ = TransportState::CLOSING;
    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(std::min<uint32_t>(options_.request_timeout_ms, 1000));
    while (metrics_.provider_outstanding_requests.load() != 0 &&
           std::chrono::steady_clock::now() < deadline) {
        poll_once_locked();
        refresh_aggregates_locked();
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    if (metrics_.provider_outstanding_requests.load() != 0) {
        for (auto& item : requests_) {
            if (!item.second.is_aggregate && item.second.state == RequestState::POSTED) {
                finalize_locked(item.second, RequestState::CANCELLED_PROVIDER_OUTSTANDING,
                            static_cast<int>(ErrorCode::ERR_CLOSED), 0,
                            "cancelled by close deadline");
            }
        }
        refresh_aggregates_locked();
        metrics_.close_errors.fetch_add(1);
        throw std::runtime_error("ERR_BUSY: close drain deadline exceeded with " +
                                 std::to_string(metrics_.provider_outstanding_requests.load()) +
                                 " provider request(s) outstanding");
    }
    refresh_aggregates_locked();
    std::vector<std::string> errors;
    cleanup_locked(errors);
    state_ = TransportState::CLOSED;
    if (!errors.empty()) {
        metrics_.close_errors.fetch_add(1);
        throw std::runtime_error("ERR_CLOSED: " + join_errors(errors));
    }
}

uint64_t UrmaTransport::next_request_id() {
    return request_seq_.fetch_add(1, std::memory_order_relaxed);
}

void TransportMetrics::record_write(uint64_t, size_t bytes) {
    submitted_requests.fetch_add(1); inflight_requests.fetch_add(1);
    submitted_bytes.fetch_add(bytes); write_requests.fetch_add(1); write_bytes.fetch_add(bytes);
}
void TransportMetrics::record_read(uint64_t, size_t bytes) {
    submitted_requests.fetch_add(1); inflight_requests.fetch_add(1);
    submitted_bytes.fetch_add(bytes); read_requests.fetch_add(1); read_bytes.fetch_add(bytes);
}
void TransportMetrics::record_send(uint64_t, size_t bytes) {
    submitted_requests.fetch_add(1); inflight_requests.fetch_add(1);
    submitted_bytes.fetch_add(bytes); send_requests.fetch_add(1); send_bytes.fetch_add(bytes);
}
void TransportMetrics::record_completion(uint64_t, bool success) {
    if (success) completed_requests.fetch_add(1); else failed_requests.fetch_add(1);
    if (inflight_requests.load() != 0) inflight_requests.fetch_sub(1);
}
void TransportMetrics::record_timeout(uint64_t) {
    timed_out_requests.fetch_add(1);
    if (inflight_requests.load() != 0) inflight_requests.fetch_sub(1);
}
void TransportMetrics::record_provider_post(uint64_t) {
    provider_outstanding_requests.fetch_add(1);
}
void TransportMetrics::record_provider_drained(uint64_t) {
    provider_drained_requests.fetch_add(1);
    if (provider_outstanding_requests.load() != 0) {
        provider_outstanding_requests.fetch_sub(1);
    }
}
void TransportMetrics::record_chunked_block(uint64_t count) {
    logical_blocks.fetch_add(1); chunked_blocks.fetch_add(1); chunks.fetch_add(count);
}

}  // namespace spark_urma
