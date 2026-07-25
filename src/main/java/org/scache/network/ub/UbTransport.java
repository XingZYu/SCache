package org.scache.network.ub;

import java.nio.ByteBuffer;

/**
 * Backend-neutral data-operation contract used below {@code UBBlockTransferService}.
 *
 * <p>One {@link #read} or {@link #write} call represents one logical operation and returns one
 * aggregate request id. A backend may split that operation internally, but it must expose only one
 * terminal result through {@link #waitFor}. Block identity, leases, generations, CRC, and Spark
 * storage types deliberately remain above this interface.</p>
 */
public interface UbTransport extends AutoCloseable {
    UbTransportCapabilities capabilities();

    /** Return a defensive copy of the backend-specific endpoint payload for control exchange. */
    byte[] localEndpoint();

    /** Connect or reconnect to an opaque endpoint produced by a peer of the same backend. */
    void connect(byte[] remoteEndpoint);

    UbRegisteredBuffer registerBuffer(ByteBuffer buffer);

    void unregisterBuffer(UbRegisteredBuffer buffer);

    long read(RemoteBuffer remote, long remoteOffset, ByteBuffer destination,
              int localOffset, int length);

    long write(RemoteBuffer remote, long remoteOffset, ByteBuffer source,
               int localOffset, int length);

    /**
     * Wait for the aggregate request. Returns zero on success, -1 on timeout, and less than -1 on
     * backend error. A backend may return an already retained terminal result again; callers must
     * not depend on indefinite retention after later requests have advanced its terminal cache.
     */
    int waitFor(long requestId, int timeoutMs);

    UbTransportMetrics metrics();

    /**
     * Drop idle peer registration imports at an application-release boundary.
     * Backends without provider-side import handles have nothing to release.
     */
    default void releaseCachedRemoteImports() {}

    boolean isClosed();

    /** Closing is idempotent; a successful close leaves no registered region or in-flight request. */
    @Override
    void close();
}
