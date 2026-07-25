// SPDX-License-Identifier: Apache-2.0
// SparkUrma: JNI bridge between Java (org.scache.network.ub.*) and
// the C++ URMA Transport (urma_transport.h).
//
// Java classes: org.scache.network.ub.UrmaNative (native methods)
//              org.scache.network.ub.UrmaTransport (Java wrapper)

#include <jni.h>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <memory>

#include "urma_transport.h"
#include "urma_types.h"

using namespace spark_urma;

// ---- Helper: get transport pointer from Java long handle ----
static UrmaTransport* get_transport(jlong handle) {
    if (handle == 0) {
        throw std::runtime_error("Null native handle");
    }
    return reinterpret_cast<UrmaTransport*>(handle);
}

// ---- Throw Java exception with message ----
static void throw_exception(JNIEnv* env, const char* className, const std::string& msg) {
    jclass cls = env->FindClass(className);
    if (cls) {
        env->ThrowNew(cls, msg.c_str());
    }
}

static void throw_mapped(JNIEnv* env, const std::string& message) {
    if (message.rfind("ERR_BOUNDS", 0) == 0 || message.rfind("ERR_INVALID_ARG", 0) == 0 ||
        message.rfind("ERR_REGISTRATION", 0) == 0 || message.rfind("ERR_OWNER", 0) == 0) {
        throw_exception(env, "org/scache/network/ub/UrmaProtocolException", message);
    } else if (message.rfind("ERR_CONNECTION", 0) == 0) {
        throw_exception(env, "org/scache/network/ub/UrmaConnectionException", message);
    } else if (message.rfind("ERR_COMPLETION", 0) == 0) {
        throw_exception(env, "org/scache/network/ub/UrmaCompletionException", message);
    } else {
        throw_exception(env, "org/scache/network/ub/UrmaNativeException", message);
    }
}

#define THROW_URMA(env, msg) throw_mapped(env, msg)
#define THROW_INIT(env, msg) throw_exception(env, "org/scache/network/ub/UrmaInitializationException", msg)
#define THROW_CONN(env, msg) throw_exception(env, "org/scache/network/ub/UrmaConnectionException", msg)
#define THROW_TIMEOUT(env, msg) throw_exception(env, "org/scache/network/ub/UrmaTimeoutException", msg)
#define THROW_COMPLETION(env, msg) throw_exception(env, "org/scache/network/ub/UrmaCompletionException", msg)
#define THROW_PROTOCOL(env, msg) throw_exception(env, "org/scache/network/ub/UrmaProtocolException", msg)

static void validate_direct_range(JNIEnv* env, jobject buffer, jint offset, jint length,
                                  const char* label, void** address) {
    if (buffer == nullptr) throw std::runtime_error(std::string(label) + " buffer is null");
    if (offset < 0 || length < 0) throw std::runtime_error(std::string(label) + " offset/length is negative");
    void* base = env->GetDirectBufferAddress(buffer);
    jlong capacity = env->GetDirectBufferCapacity(buffer);
    if (base == nullptr || capacity < 0) throw std::runtime_error(std::string(label) + " must be DirectByteBuffer");
    if (static_cast<jlong>(offset) > capacity || static_cast<jlong>(length) > capacity - offset)
        throw std::runtime_error(std::string(label) + " offset/length exceeds capacity");
    *address = static_cast<uint8_t*>(base) + offset;
}

static jlongArray metrics_array(JNIEnv* env, const TransportMetrics& m) {
    jlong values[32] = {
        static_cast<jlong>(m.submitted_requests.load()), static_cast<jlong>(m.completed_requests.load()),
        static_cast<jlong>(m.failed_requests.load()), static_cast<jlong>(m.timed_out_requests.load()),
        static_cast<jlong>(m.write_bytes.load()), static_cast<jlong>(m.read_bytes.load()),
        static_cast<jlong>(m.send_bytes.load()), static_cast<jlong>(m.checksum_errors.load()),
        static_cast<jlong>(m.chunked_blocks.load()), static_cast<jlong>(m.chunks.load()),
        static_cast<jlong>(m.inflight_requests.load()), static_cast<jlong>(m.submitted_bytes.load()),
        static_cast<jlong>(m.completed_bytes.load()), static_cast<jlong>(m.late_completions.load()),
        static_cast<jlong>(m.unknown_waits.load()), static_cast<jlong>(m.write_requests.load()),
        static_cast<jlong>(m.read_requests.load()), static_cast<jlong>(m.send_requests.load()),
        static_cast<jlong>(m.logical_blocks.load()), static_cast<jlong>(m.registered_regions.load()),
        static_cast<jlong>(m.unregistered_regions.load()), static_cast<jlong>(m.active_regions.load()),
        static_cast<jlong>(m.imported_regions.load()), static_cast<jlong>(m.unimported_regions.load()),
        static_cast<jlong>(m.active_imports.load()), static_cast<jlong>(m.init_calls.load()),
        static_cast<jlong>(m.uninit_calls.load()), static_cast<jlong>(m.active_transports.load()),
        static_cast<jlong>(m.close_errors.load()), static_cast<jlong>(m.tcp_payload_bytes.load()),
        static_cast<jlong>(m.provider_outstanding_requests.load()),
        static_cast<jlong>(m.provider_drained_requests.load())};
    jlongArray result = env->NewLongArray(32);
    if (result != nullptr) env->SetLongArrayRegion(result, 0, 32, values);
    return result;
}

// ---- JNI method implementations ----

extern "C" {

// UrmaNative.nativeInit(String deviceName, int queueDepth, int maxChunkBytes, boolean strict, String wireRole)
JNIEXPORT jlong JNICALL
Java_org_scache_network_ub_UrmaNative_nativeInit(
    JNIEnv* env, jclass clazz, jstring deviceName,
    jint queueDepth, jint maxChunkBytes, jboolean strict, jstring wireRole)
{
    (void)clazz;
    try {
        if (deviceName == nullptr || wireRole == nullptr || queueDepth <= 0 ||
            maxChunkBytes <= 0) {
            throw std::runtime_error("invalid nativeInit arguments");
        }
        const char* dev = env->GetStringUTFChars(deviceName, nullptr);
        if (dev == nullptr) return 0;
        const char* role = env->GetStringUTFChars(wireRole, nullptr);
        if (role == nullptr) { env->ReleaseStringUTFChars(deviceName, dev); return 0; }
        TransportOptions opts;
        opts.device_name = dev;
        opts.wire_role = role ? role : "";
        opts.queue_depth = static_cast<uint32_t>(queueDepth);
        opts.max_chunk_bytes = static_cast<uint32_t>(maxChunkBytes);
        opts.strict_mode = (strict == JNI_TRUE);
        env->ReleaseStringUTFChars(deviceName, dev);
        env->ReleaseStringUTFChars(wireRole, role);

        std::unique_ptr<UrmaTransport> transport(new UrmaTransport());
        transport->init(opts);
        return reinterpret_cast<jlong>(transport.release());
    } catch (const std::exception& e) {
        THROW_INIT(env, e.what());
        return 0;
    }
}

// UrmaNative.nativeConnect(long handle, byte[] eid, int uasid, int jettyId,
//                           long segAddr, long segLen, int segToken)
JNIEXPORT void JNICALL
Java_org_scache_network_ub_UrmaNative_nativeConnect(
    JNIEnv* env, jclass clazz, jlong handle,
    jbyteArray eid, jint uasid, jint jettyId, jint remoteMaxChunkBytes,
    jint segmentUasid, jlong segmentGeneration,
    jlong segAddr, jlong segLen, jint segToken)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);

        EndpointDescriptor remote;
        remote.uasid = static_cast<uint32_t>(uasid);
        remote.jetty_id = static_cast<uint32_t>(jettyId);
        remote.segment_address = static_cast<uint64_t>(segAddr);
        remote.segment_length = static_cast<uint64_t>(segLen);
        remote.segment_uasid = static_cast<uint32_t>(segmentUasid);
        remote.segment_token = static_cast<uint32_t>(segToken);
        remote.segment_generation = static_cast<uint64_t>(segmentGeneration);

        if (eid == nullptr || env->GetArrayLength(eid) != 16 ||
            remoteMaxChunkBytes <= 0 || segAddr < 0 || segLen < 0)
            throw std::runtime_error("endpoint EID must be exactly 16 bytes and segment values non-negative");
        remote.max_chunk_bytes = static_cast<uint32_t>(remoteMaxChunkBytes);
        env->GetByteArrayRegion(eid, 0, 16, reinterpret_cast<jbyte*>(remote.eid));
        if (env->ExceptionCheck()) return;

        transport->connect(remote);
    } catch (const std::exception& e) {
        THROW_CONN(env, e.what());
    }
}

// UrmaNative.nativeGetLocalEndpoint(long handle) -> byte[] (serialized EndpointDescriptor)
JNIEXPORT jbyteArray JNICALL
Java_org_scache_network_ub_UrmaNative_nativeGetLocalEndpoint(
    JNIEnv* env, jclass clazz, jlong handle)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);
        const auto& ep = transport->local_endpoint();

        EndpointDescriptor net_ep = ep;
        net_ep.to_network();
        jbyteArray result = env->NewByteArray(static_cast<jsize>(sizeof(net_ep)));
        env->SetByteArrayRegion(result, 0, static_cast<jsize>(sizeof(net_ep)),
                                reinterpret_cast<const jbyte*>(&net_ep));
        return result;
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
        return nullptr;
    }
}

// UrmaNative.nativeClose(long handle)
JNIEXPORT jlongArray JNICALL
Java_org_scache_network_ub_UrmaNative_nativeClose(
    JNIEnv* env, jclass clazz, jlong handle)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);
        transport->close();
        jlongArray result = metrics_array(env, transport->metrics());
        delete transport;
        return result;
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
        return nullptr;
    }
}

// UrmaNative.nativeRegisterBuffer(long handle, ByteBuffer directBuffer) ->
// [handle, remoteAddress, length, token, providerGeneration]
JNIEXPORT jlongArray JNICALL
Java_org_scache_network_ub_UrmaNative_nativeRegisterBuffer(
    JNIEnv* env, jclass clazz, jlong handle, jobject buffer)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);

        if (buffer == nullptr) throw std::runtime_error("buffer is null");
        void* addr = env->GetDirectBufferAddress(buffer);
        jlong cap = env->GetDirectBufferCapacity(buffer);

        if (!addr || cap <= 0) throw std::runtime_error("must use non-empty ByteBuffer.allocateDirect()");

        auto region = transport->register_region(addr, static_cast<size_t>(cap));
        jlong values[5] = {static_cast<jlong>(region.handle),
                           static_cast<jlong>(region.remote_address),
                           static_cast<jlong>(region.length),
                           static_cast<jlong>(region.token),
                           static_cast<jlong>(region.generation)};
        jlongArray result = env->NewLongArray(5);
        if (result != nullptr) env->SetLongArrayRegion(result, 0, 5, values);
        return result;
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
        return nullptr;
    }
}

// UrmaNative.nativeUnregisterBuffer(long handle, long regionHandle)
JNIEXPORT void JNICALL
Java_org_scache_network_ub_UrmaNative_nativeUnregisterBuffer(
    JNIEnv* env, jclass clazz, jlong handle, jlong regionHandle)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);
        transport->unregister_region(static_cast<uint64_t>(regionHandle));
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
    }
}

// UrmaNative.nativeReleaseCachedRemoteImports(long handle)
JNIEXPORT void JNICALL
Java_org_scache_network_ub_UrmaNative_nativeReleaseCachedRemoteImports(
    JNIEnv* env, jclass clazz, jlong handle)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);
        transport->release_cached_remote_imports();
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
    }
}

// UrmaNative.nativeWrite(long handle, long remoteAddr, long remoteLen, int remoteToken,
//                         long remoteGeneration,
//                         ByteBuffer source, int offset, int length) -> long requestId
JNIEXPORT jlong JNICALL
Java_org_scache_network_ub_UrmaNative_nativeWrite(
    JNIEnv* env, jclass clazz, jlong handle,
    jlong remoteAddr, jlong remoteLen, jint remoteToken, jlong remoteGeneration,
    jobject source, jint offset, jint length)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);

        void* addr = nullptr;
        validate_direct_range(env, source, offset, length, "source", &addr);

        RemoteRegion remote;
        remote.remote_address = static_cast<uint64_t>(remoteAddr);
        remote.length = static_cast<uint64_t>(remoteLen);
        remote.token = static_cast<uint64_t>(remoteToken);
        remote.generation = static_cast<uint64_t>(remoteGeneration);

        uint64_t req_id = transport->write(remote, addr, static_cast<size_t>(length));
        return static_cast<jlong>(req_id);
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
        return 0;
    }
}

// UrmaNative.nativeRead(long handle, long remoteAddr, long remoteLen, int remoteToken,
//                        long remoteGeneration,
//                        ByteBuffer destination, int offset, int length) -> long requestId
JNIEXPORT jlong JNICALL
Java_org_scache_network_ub_UrmaNative_nativeRead(
    JNIEnv* env, jclass clazz, jlong handle,
    jlong remoteAddr, jlong remoteLen, jint remoteToken, jlong remoteGeneration,
    jobject destination, jint offset, jint length)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);

        void* addr = nullptr;
        validate_direct_range(env, destination, offset, length, "destination", &addr);

        RemoteRegion remote;
        remote.remote_address = static_cast<uint64_t>(remoteAddr);
        remote.length = static_cast<uint64_t>(remoteLen);
        remote.token = static_cast<uint64_t>(remoteToken);
        remote.generation = static_cast<uint64_t>(remoteGeneration);

        uint64_t req_id = transport->read(remote, addr, static_cast<size_t>(length));
        return static_cast<jlong>(req_id);
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
        return 0;
    }
}

// UrmaNative.nativeSend(long handle, ByteBuffer message, int offset, int length) -> long requestId
JNIEXPORT jlong JNICALL
Java_org_scache_network_ub_UrmaNative_nativeSend(
    JNIEnv* env, jclass clazz, jlong handle,
    jobject message, jint offset, jint length)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);

        void* addr = nullptr;
        validate_direct_range(env, message, offset, length, "message", &addr);

        uint64_t req_id = transport->send(addr, static_cast<size_t>(length));
        return static_cast<jlong>(req_id);
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
        return 0;
    }
}

// UrmaNative.nativeWait(long handle, long requestId, int timeoutMs) -> int (0=success, -1=timeout, <0=error)
JNIEXPORT jint JNICALL
Java_org_scache_network_ub_UrmaNative_nativeWait(
    JNIEnv* env, jclass clazz, jlong handle, jlong requestId, jint timeoutMs)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);
        auto result = transport->wait(static_cast<uint64_t>(requestId),
                                       static_cast<uint32_t>(timeoutMs));
        if (result.status == static_cast<int>(ErrorCode::ERR_TIMEOUT)) {
            THROW_TIMEOUT(env, result.message); return result.status;
        }
        if (result.status != 0) {
            THROW_COMPLETION(env, result.message.empty() ? "URMA request failed" : result.message);
            return result.status;
        }
        return 0;
    } catch (const std::exception& e) {
        const std::string message(e.what());
        if (message.rfind("ERR_UNKNOWN_REQUEST", 0) == 0 ||
            message.rfind("ERR_INVALID_ARG", 0) == 0) {
            THROW_PROTOCOL(env, message);
        } else {
            THROW_COMPLETION(env, message);
        }
        return -1;
    }
}

// UrmaNative.nativeWriteChunked(long handle, long remoteAddr, long remoteLen, int remoteToken,
//                                long remoteGeneration,
//                                ByteBuffer source, int offset, int totalLength) -> long requestId
JNIEXPORT jlong JNICALL
Java_org_scache_network_ub_UrmaNative_nativeWriteChunked(
    JNIEnv* env, jclass clazz, jlong handle,
    jlong remoteAddr, jlong remoteLen, jint remoteToken, jlong remoteGeneration,
    jobject source, jint offset, jint totalLength)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);

        void* addr = nullptr;
        validate_direct_range(env, source, offset, totalLength, "source", &addr);

        RemoteRegion remote;
        remote.remote_address = static_cast<uint64_t>(remoteAddr);
        remote.length = static_cast<uint64_t>(remoteLen);
        remote.token = static_cast<uint64_t>(remoteToken);
        remote.generation = static_cast<uint64_t>(remoteGeneration);

        uint64_t req_id = transport->write_chunked(remote, addr,
                                                     static_cast<size_t>(totalLength));
        return static_cast<jlong>(req_id);
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
        return 0;
    }
}

// UrmaNative.nativeReadChunked(long handle, long remoteAddr, long remoteLen, int remoteToken,
//                               long remoteGeneration,
//                               ByteBuffer dest, int offset, int totalLength) -> long requestId
JNIEXPORT jlong JNICALL
Java_org_scache_network_ub_UrmaNative_nativeReadChunked(
    JNIEnv* env, jclass clazz, jlong handle,
    jlong remoteAddr, jlong remoteLen, jint remoteToken, jlong remoteGeneration,
    jobject dest, jint offset, jint totalLength)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);

        void* addr = nullptr;
        validate_direct_range(env, dest, offset, totalLength, "destination", &addr);

        RemoteRegion remote;
        remote.remote_address = static_cast<uint64_t>(remoteAddr);
        remote.length = static_cast<uint64_t>(remoteLen);
        remote.token = static_cast<uint64_t>(remoteToken);
        remote.generation = static_cast<uint64_t>(remoteGeneration);

        uint64_t req_id = transport->read_chunked(remote, addr,
                                                    static_cast<size_t>(totalLength));
        return static_cast<jlong>(req_id);
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
        return 0;
    }
}

// UrmaNative.nativeGetMetrics(long handle) -> long[]
JNIEXPORT jlongArray JNICALL
Java_org_scache_network_ub_UrmaNative_nativeGetMetrics(
    JNIEnv* env, jclass clazz, jlong handle)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);
        const auto& m = transport->metrics();

        return metrics_array(env, m);
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
        return nullptr;
    }
}

JNIEXPORT void JNICALL
Java_org_scache_network_ub_UrmaNative_nativeShutdownProcessRuntime(
    JNIEnv* env, jclass clazz)
{
    (void)clazz;
    try {
        UrmaTransport::shutdown_process_runtime();
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
    }
}

JNIEXPORT jlongArray JNICALL
Java_org_scache_network_ub_UrmaNative_nativeGetProcessRuntimeMetrics(
    JNIEnv* env, jclass clazz)
{
    (void)clazz;
    const ProcessRuntimeMetrics metrics = UrmaTransport::process_runtime_metrics();
    jlong values[5] = {
        static_cast<jlong>(metrics.generation),
        static_cast<jlong>(metrics.init_calls),
        static_cast<jlong>(metrics.uninit_calls),
        static_cast<jlong>(metrics.active_transports),
        metrics.initialized ? 1 : 0};
    jlongArray result = env->NewLongArray(5);
    if (result != nullptr) env->SetLongArrayRegion(result, 0, 5, values);
    return result;
}

}  // extern "C"
