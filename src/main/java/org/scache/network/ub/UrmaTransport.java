package org.scache.network.ub;

import java.nio.ByteBuffer;

/**
 * Java wrapper around the native C++ UrmaTransport.
 *
 * Validation Rules:
 * - Only DirectByteBuffer (ByteBuffer.allocateDirect) is accepted.
 * - HeapByteBuffer and byte[] MUST be rejected explicitly.
 * - All offset/length combinations are bounds-checked.
 * - Double close() is safe.
 */
public final class UrmaTransport implements AutoCloseable {

    private long nativeHandle;
    private volatile boolean closed = false;

    private String deviceName;
    private int maxChunkBytes;
    private long ownerEpoch;
    private TransportMetrics closeMetrics;

    private UrmaTransport() {}

    /**
     * Create and initialize a URMA transport.
     *
     * @param deviceName   URMA device name (e.g. "openurma0")
     * @param queueDepth   Completion queue depth
     * @param maxChunkBytes Maximum bytes per single URMA operation (default 4096 for Tier S)
     * @param strict       If true, reject payloads > maxChunkBytes (use chunked ops instead)
     */
    public static UrmaTransport open(String deviceName, int queueDepth,
                                      int maxChunkBytes, boolean strict)
            throws UrmaInitializationException {
        String launcherRole = System.getenv("OPENURMA_WIRE_ROLE");
        if (launcherRole == null || launcherRole.isBlank()) {
            throw new UrmaInitializationException(
                "OPENURMA_WIRE_ROLE must be set by the launcher before JVM startup");
        }
        return open(deviceName, queueDepth, maxChunkBytes, strict, launcherRole);
    }

    public static UrmaTransport open(String deviceName, int queueDepth,
                                      int maxChunkBytes, boolean strict,
                                      String wireRole)
            throws UrmaInitializationException {
        UrmaNative.ensureLoaded();

        if (deviceName == null || deviceName.isBlank()) {
            throw new UrmaInitializationException("deviceName must not be null or blank");
        }
        if (queueDepth <= 0) {
            throw new UrmaInitializationException("queueDepth must be > 0, got " + queueDepth);
        }
        if (maxChunkBytes <= 0) {
            throw new UrmaInitializationException("maxChunkBytes must be > 0, got " + maxChunkBytes);
        }
        if (maxChunkBytes > 4096) {
            throw new UrmaInitializationException("Tier S maxChunkBytes must be <= 4096");
        }
        if (wireRole == null || (!wireRole.equals("listen") && !wireRole.equals("connect"))) {
            throw new UrmaInitializationException("wireRole must be listen or connect");
        }
        String launcherRole = System.getenv("OPENURMA_WIRE_ROLE");
        if (launcherRole == null || !launcherRole.equals(wireRole)) {
            throw new UrmaInitializationException(
                "launcher OPENURMA_WIRE_ROLE does not match expected role " + wireRole);
        }

        UrmaTransport t = new UrmaTransport();
        t.deviceName = deviceName;
        t.maxChunkBytes = maxChunkBytes;
        t.ownerEpoch = System.nanoTime();
        t.nativeHandle = UrmaNative.nativeInit(deviceName, queueDepth, maxChunkBytes, strict, wireRole);
        return t;
    }

    /** Connect to a remote endpoint. */
    public void connect(EndpointDescriptor remote) throws UrmaConnectionException {
        checkNotClosed();
        if (remote == null) throw new UrmaConnectionException("remote endpoint must not be null");
        remote.validate();
        UrmaNative.nativeConnect(nativeHandle, remote.eid, remote.uasid, remote.jettyId,
                                  remote.segmentAddress, remote.segmentLength, remote.segmentToken);
    }

    /** Get this transport's local endpoint for exchange via control channel. */
    public EndpointDescriptor getLocalEndpoint() {
        checkNotClosed();
        byte[] raw = UrmaNative.nativeGetLocalEndpoint(nativeHandle);
        return EndpointDescriptor.fromBytes(raw);
    }

    // ---- Buffer Registration ----

    /**
     * Register a DirectByteBuffer as a URMA segment.
     * @throws UrmaException if buffer is not direct
     */
    public RegisteredBuffer registerBuffer(ByteBuffer buffer) {
        checkNotClosed();
        checkDirect(buffer);
        long[] descriptor = UrmaNative.nativeRegisterBuffer(nativeHandle, buffer);
        if (descriptor == null || descriptor.length != 4) {
            throw new UrmaException("native registration returned an invalid descriptor");
        }
        return new RegisteredBuffer(this, descriptor[0], buffer,
                                    descriptor[1], descriptor[2], descriptor[3]);
    }

    public void unregisterBuffer(RegisteredBuffer buffer) {
        checkNotClosed();
        if (buffer == null) throw new UrmaException("registered buffer must not be null");
        buffer.checkOwner(this);
        if (!buffer.registered) throw new UrmaException("buffer is already unregistered");
        UrmaNative.nativeUnregisterBuffer(nativeHandle, buffer.handle);
        buffer.registered = false;
    }

    // ---- Write ----

    /**
     * Write data to a remote URMA segment.
     * @return request ID for wait()
     */
    public long write(RemoteBuffer remote, ByteBuffer source, int offset, int length) {
        checkNotClosed();
        checkRemote(remote);
        checkDirect(source);
        checkBounds(source, offset, length);

        if (closed) throw new UrmaException("Transport closed");
        return UrmaNative.nativeWrite(nativeHandle,
            remote.remoteAddress, remote.length, remote.token,
            source, offset, length);
    }

    // ---- Read ----

    /**
     * Read data from a remote URMA segment.
     * @return request ID for wait()
     */
    public long read(RemoteBuffer remote, ByteBuffer dest, int offset, int length) {
        checkNotClosed();
        checkRemote(remote);
        checkDirect(dest);
        checkBounds(dest, offset, length);

        return UrmaNative.nativeRead(nativeHandle,
            remote.remoteAddress, remote.length, remote.token,
            dest, offset, length);
    }

    // ---- Send ----

    /**
     * Send a message via URMA SEND.
     * @return request ID for wait()
     */
    public long send(ByteBuffer message, int offset, int length) {
        checkNotClosed();
        checkDirect(message);
        checkBounds(message, offset, length);

        return UrmaNative.nativeSend(nativeHandle, message, offset, length);
    }

    // ---- Chunked operations ----

    /**
     * Write a large block split into <= maxChunkBytes chunks.
     */
    public long writeChunked(RemoteBuffer remote, ByteBuffer source, int offset, int totalLength) {
        checkNotClosed();
        checkRemote(remote);
        checkDirect(source);
        checkBounds(source, offset, totalLength);

        return UrmaNative.nativeWriteChunked(nativeHandle,
            remote.remoteAddress, remote.length, remote.token,
            source, offset, totalLength);
    }

    /**
     * Read a large block split into chunks.
     */
    public long readChunked(RemoteBuffer remote, ByteBuffer dest, int offset, int totalLength) {
        checkNotClosed();
        checkRemote(remote);
        checkDirect(dest);
        checkBounds(dest, offset, totalLength);

        return UrmaNative.nativeReadChunked(nativeHandle,
            remote.remoteAddress, remote.length, remote.token,
            dest, offset, totalLength);
    }

    // ---- Completion ----

    /**
     * Wait for a specific request to complete.
     * @param requestId the request ID returned by write/read/send
     * @param timeoutMs timeout in milliseconds
     * @return 0 on success, -1 on timeout, < -1 on error
     */
    public int waitFor(long requestId, int timeoutMs) {
        checkNotClosed();
        if (requestId <= 0) throw new UrmaException("requestId must be > 0");
        if (timeoutMs <= 0) throw new UrmaException("timeoutMs must be > 0");
        return UrmaNative.nativeWait(nativeHandle, requestId, timeoutMs);
    }

    // ---- Metrics ----

    public TransportMetrics getMetrics() {
        checkNotClosed();
        long[] raw = UrmaNative.nativeGetMetrics(nativeHandle);
        return new TransportMetrics(raw);
    }

    // ---- Cleanup ----

    @Override
    public void close() {
        if (closed) return;
        if (nativeHandle != 0) {
            closeMetrics = new TransportMetrics(UrmaNative.nativeClose(nativeHandle));
            nativeHandle = 0;
        }
        closed = true;
    }

    public boolean isClosed() { return closed; }
    public TransportMetrics closeMetrics() {
        if (!closed || closeMetrics == null) throw new UrmaException("transport has not closed successfully");
        return closeMetrics;
    }
    public String deviceName() { return deviceName; }
    public int maxChunkBytes() { return maxChunkBytes; }

    static ProcessRuntimeMetrics processRuntimeMetrics() {
        UrmaNative.ensureLoaded();
        return new ProcessRuntimeMetrics(UrmaNative.nativeGetProcessRuntimeMetrics());
    }

    static void shutdownProcessRuntime() {
        UrmaNative.ensureLoaded();
        UrmaNative.nativeShutdownProcessRuntime();
    }

    // ---- Internal validation ----

    private void checkNotClosed() {
        if (closed) throw new UrmaException("Transport is closed");
        if (nativeHandle == 0) throw new UrmaException("Invalid native handle");
    }

    static void checkDirect(ByteBuffer buf) {
        if (buf == null) {
            throw new UrmaException("ByteBuffer must not be null");
        }
        if (!buf.isDirect()) {
            throw new UrmaException(
                "Only DirectByteBuffer is supported. Use ByteBuffer.allocateDirect(). " +
                "HeapByteBuffer and byte[] are rejected because they can be moved by GC.");
        }
    }

    static void checkBounds(ByteBuffer buf, int offset, int length) {
        if (offset < 0) {
            throw new UrmaException("offset must be >= 0, got " + offset);
        }
        if (length < 0) {
            throw new UrmaException("length must be >= 0, got " + length);
        }
        if (offset > buf.capacity() || length > buf.capacity() - offset) {
            throw new UrmaException(
                "offset(" + offset + ") + length(" + length +
                ") exceeds capacity(" + buf.capacity() + ")");
        }
    }

    private void checkRemote(RemoteBuffer remote) {
        if (remote == null) throw new UrmaException("remote buffer must not be null");
        if (remote.remoteAddress <= 0 || remote.length <= 0 || remote.token < 0) {
            throw new UrmaException("invalid remote region descriptor");
        }
    }

    // ---- Inner types ----

    public static class RegisteredBuffer {
        private final UrmaTransport owner;
        public final long handle;
        public final ByteBuffer buffer;
        public final long remoteAddress;
        public final long length;
        public final long token;
        private volatile boolean registered = true;
        RegisteredBuffer(UrmaTransport owner, long handle, ByteBuffer buffer,
                         long remoteAddress, long length, long token) {
            this.owner = owner;
            this.handle = handle;
            this.buffer = buffer;
            this.remoteAddress = remoteAddress;
            this.length = length;
            this.token = token;
        }
        void checkOwner(UrmaTransport candidate) {
            if (owner != candidate) throw new UrmaException("registered buffer belongs to another transport");
        }
        public boolean isRegistered() { return registered; }
        public RemoteBuffer descriptor() {
            if (!registered) throw new UrmaException("buffer is unregistered");
            return new RemoteBuffer(remoteAddress, length, (int) token);
        }
    }
}
