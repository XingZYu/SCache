package org.scache.network.ub;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Backend-neutral adapter over the existing JNI-backed {@link UrmaTransport}. */
public final class RealUbTransport implements UbTransport {
    private final UrmaTransport delegate;
    private final UbTransportCapabilities capabilities;
    private final LogicalMetrics logicalMetrics = new LogicalMetrics();

    private RealUbTransport(UrmaTransport delegate, int queueDepth) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.capabilities = capabilitiesFor(queueDepth, delegate.maxChunkBytes());
    }

    public static RealUbTransport open(String deviceName, int queueDepth, int maxChunkBytes,
                                       boolean strict, String wireRole) {
        UrmaTransport transport =
            UrmaTransport.open(deviceName, queueDepth, maxChunkBytes, strict, wireRole);
        return new RealUbTransport(transport, queueDepth);
    }

    static UbTransportCapabilities capabilitiesFor(int queueDepth, int maxChunkBytes) {
        return new UbTransportCapabilities(
            "real-urma", UbTransportCapabilities.BackendClass.REAL,
            true, true, Integer.MAX_VALUE, maxChunkBytes, queueDepth, 1, true, false);
    }

    @Override
    public UbTransportCapabilities capabilities() {
        return capabilities;
    }

    @Override
    public byte[] localEndpoint() {
        return defensiveCopy(delegate.getLocalEndpoint().toBytes());
    }

    @Override
    public void connect(byte[] remoteEndpoint) {
        if (remoteEndpoint == null) throw new UrmaProtocolException("remote endpoint must not be null");
        delegate.connect(EndpointDescriptor.fromBytes(defensiveCopy(remoteEndpoint)));
    }

    static byte[] defensiveCopy(byte[] value) {
        if (value == null) throw new IllegalArgumentException("endpoint must not be null");
        return value.clone();
    }

    @Override
    public UbRegisteredBuffer registerBuffer(ByteBuffer buffer) {
        UrmaTransport.RegisteredBuffer registered = delegate.registerBuffer(buffer);
        if (registered.token < 0 || registered.token > Integer.MAX_VALUE) {
            try {
                delegate.unregisterBuffer(registered);
            } catch (RuntimeException cleanup) {
                // Preserve the descriptor error as the primary failure.
            }
            throw new UrmaException("registered buffer token does not fit the neutral descriptor");
        }
        return new UbRegisteredBuffer(this, registered, registered.handle, registered.generation,
            registered.buffer, registered.remoteAddress, registered.length, (int) registered.token);
    }

    @Override
    public void unregisterBuffer(UbRegisteredBuffer buffer) {
        if (buffer == null) throw new UrmaException("registered buffer must not be null");
        buffer.requireOwner(this);
        Object backend = buffer.backendHandle();
        if (!(backend instanceof UrmaTransport.RegisteredBuffer)) {
            throw new UrmaException("registered buffer has an invalid real backend handle");
        }
        if (!buffer.isRegistered()) throw new UrmaException("buffer is already unregistered");
        delegate.unregisterBuffer((UrmaTransport.RegisteredBuffer) backend);
        if (!buffer.markUnregistered()) {
            throw new UrmaException("buffer was concurrently unregistered");
        }
    }

    @Override
    public long read(RemoteBuffer remote, long remoteOffset, ByteBuffer destination,
                     int localOffset, int length) {
        long requestId = delegate.readChunked(window(remote, remoteOffset, length),
            destination, localOffset, length);
        logicalMetrics.recordSubmitted(requestId, length);
        return requestId;
    }

    @Override
    public long write(RemoteBuffer remote, long remoteOffset, ByteBuffer source,
                      int localOffset, int length) {
        long requestId = delegate.writeChunked(window(remote, remoteOffset, length),
            source, localOffset, length);
        logicalMetrics.recordSubmitted(requestId, length);
        return requestId;
    }

    static RemoteBuffer window(RemoteBuffer remote, long remoteOffset, int length) {
        if (remote == null) throw new UrmaException("remote buffer must not be null");
        if (remote.remoteAddress <= 0 || remote.length <= 0 || remote.token < 0) {
            throw new UrmaException("invalid remote region descriptor");
        }
        if (remoteOffset < 0 || length < 0 || remoteOffset > remote.length
                || length > remote.length - remoteOffset) {
            throw new UrmaException("remote offset/length exceeds descriptor");
        }
        if (remote.remoteAddress > Long.MAX_VALUE - remoteOffset) {
            throw new UrmaException("remote address overflow");
        }
        return new RemoteBuffer(remote.remoteAddress + remoteOffset,
            remote.length - remoteOffset, remote.token, remote.generation);
    }

    @Override
    public int waitFor(long requestId, int timeoutMs) {
        int status = delegate.waitFor(requestId, timeoutMs);
        logicalMetrics.recordTerminal(requestId, status);
        return status;
    }

    @Override
    public UbTransportMetrics metrics() {
        return logicalMetrics.snapshot(delegate.getMetrics());
    }

    /** Adapter-owned aggregate accounting; native metrics below remain raw request counters. */
    static final class LogicalMetrics {
        private final Map<Long, Long> inflight = new HashMap<>();
        private long submittedOperations;
        private long completedOperations;
        private long failedOperations;
        private long timedOutOperations;
        private long submittedBytes;
        private long completedBytes;
        private long failedBytes;
        private long inflightBytes;

        synchronized void recordSubmitted(long requestId, long bytes) {
            if (requestId <= 0) throw new IllegalArgumentException("requestId must be positive");
            if (bytes < 0) throw new IllegalArgumentException("logical bytes must be non-negative");
            if (inflight.putIfAbsent(requestId, bytes) != null) {
                throw new IllegalStateException("duplicate aggregate request id " + requestId);
            }
            submittedOperations++;
            submittedBytes = Math.addExact(submittedBytes, bytes);
            inflightBytes = Math.addExact(inflightBytes, bytes);
        }

        /** Returns false for an unknown or already-accounted terminal request. */
        synchronized boolean recordTerminal(long requestId, int status) {
            Long bytes = inflight.remove(requestId);
            if (bytes == null) return false;
            inflightBytes = Math.subtractExact(inflightBytes, bytes);
            if (status == 0) {
                completedOperations++;
                completedBytes = Math.addExact(completedBytes, bytes);
            } else {
                failedOperations++;
                failedBytes = Math.addExact(failedBytes, bytes);
                if (status == -1) timedOutOperations++;
            }
            return true;
        }

        /** A successful backend close cancels any aggregate result the caller never consumed. */
        synchronized void recordClosed() {
            for (long bytes : inflight.values()) {
                failedOperations++;
                failedBytes = Math.addExact(failedBytes, bytes);
            }
            inflight.clear();
            inflightBytes = 0;
        }

        synchronized UbTransportMetrics snapshot(TransportMetrics nativeMetrics) {
            UbTransportMetrics.Builder builder = UbTransportMetrics.builder("real-urma")
                .operations(submittedOperations, completedOperations, failedOperations,
                    timedOutOperations, inflight.size())
                .bytes(submittedBytes, completedBytes, failedBytes, inflightBytes)
                .registeredRegions(nativeMetrics.activeRegions);
            return addNativeCounters(builder, nativeMetrics).build();
        }
    }

    private static UbTransportMetrics.Builder addNativeCounters(
            UbTransportMetrics.Builder builder, TransportMetrics nativeMetrics) {
        return builder
            .backendCounter("submittedRequests", nativeMetrics.submittedRequests)
            .backendCounter("completedRequests", nativeMetrics.completedRequests)
            .backendCounter("failedRequests", nativeMetrics.failedRequests)
            .backendCounter("timedOutRequests", nativeMetrics.timedOutRequests)
            .backendCounter("writeBytes", nativeMetrics.writeBytes)
            .backendCounter("readBytes", nativeMetrics.readBytes)
            .backendCounter("sendBytes", nativeMetrics.sendBytes)
            .backendCounter("checksumErrors", nativeMetrics.checksumErrors)
            .backendCounter("chunkedBlocks", nativeMetrics.chunkedBlocks)
            .backendCounter("chunks", nativeMetrics.chunks)
            .backendCounter("inflightRequests", nativeMetrics.inflightRequests)
            .backendCounter("submittedBytes", nativeMetrics.submittedBytes)
            .backendCounter("completedBytes", nativeMetrics.completedBytes)
            .backendCounter("logicalBlocks", nativeMetrics.logicalBlocks)
            .backendCounter("activeRegions", nativeMetrics.activeRegions)
            .backendCounter("activeImports", nativeMetrics.activeImports)
            .backendCounter("activeTransports", nativeMetrics.activeTransports)
            .backendCounter("lateCompletions", nativeMetrics.lateCompletions)
            .backendCounter("unknownWaits", nativeMetrics.unknownWaits)
            .backendCounter("writeRequests", nativeMetrics.writeRequests)
            .backendCounter("readRequests", nativeMetrics.readRequests)
            .backendCounter("sendRequests", nativeMetrics.sendRequests)
            .backendCounter("registeredRegions", nativeMetrics.registeredRegions)
            .backendCounter("unregisteredRegions", nativeMetrics.unregisteredRegions)
            .backendCounter("importedRegions", nativeMetrics.importedRegions)
            .backendCounter("unimportedRegions", nativeMetrics.unimportedRegions)
            .backendCounter("initCalls", nativeMetrics.initCalls)
            .backendCounter("uninitCalls", nativeMetrics.uninitCalls)
            .backendCounter("closeErrors", nativeMetrics.closeErrors)
            .backendCounter("providerOutstandingRequests", nativeMetrics.providerOutstandingRequests)
            .backendCounter("providerDrainedRequests", nativeMetrics.providerDrainedRequests)
            .backendCounter("tcpPayloadBytes", nativeMetrics.tcpPayloadBytes);
    }

    @Override
    public boolean isClosed() {
        return delegate.isClosed();
    }

    @Override
    public void releaseCachedRemoteImports() {
        delegate.releaseCachedRemoteImports();
    }

    @Override
    public void close() {
        delegate.close();
        logicalMetrics.recordClosed();
    }
}
