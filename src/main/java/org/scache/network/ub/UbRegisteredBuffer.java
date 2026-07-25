package org.scache.network.ub;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** Backend-neutral registered direct buffer and remotely usable descriptor. */
public final class UbRegisteredBuffer {
    private final Object ownerIdentity;
    private final Object backendHandle;
    private final AtomicBoolean registered = new AtomicBoolean(true);

    public final long handle;
    public final ByteBuffer buffer;
    public final long remoteAddress;
    public final long length;
    public final int token;
    public final long generation;

    UbRegisteredBuffer(Object ownerIdentity, Object backendHandle, long handle, ByteBuffer buffer,
                       long remoteAddress, long length, int token) {
        this(ownerIdentity, backendHandle, handle, 0L, buffer, remoteAddress, length, token);
    }

    UbRegisteredBuffer(Object ownerIdentity, Object backendHandle, long handle, long generation,
                       ByteBuffer buffer, long remoteAddress, long length, int token) {
        this.ownerIdentity = Objects.requireNonNull(ownerIdentity, "ownerIdentity");
        this.backendHandle = backendHandle;
        if (handle <= 0) throw new IllegalArgumentException("handle must be positive");
        if (buffer == null || !buffer.isDirect()) {
            throw new IllegalArgumentException("registered buffer must be a DirectByteBuffer");
        }
        if (remoteAddress < 0 || length <= 0 || length > buffer.capacity() || token < 0) {
            throw new IllegalArgumentException("invalid registered buffer descriptor");
        }
        this.handle = handle;
        this.generation = generation;
        this.buffer = buffer;
        this.remoteAddress = remoteAddress;
        this.length = length;
        this.token = token;
    }

    void requireOwner(Object candidate) {
        if (ownerIdentity != candidate) {
            throw new IllegalArgumentException("registered buffer belongs to another transport");
        }
    }

    Object backendHandle() { return backendHandle; }

    boolean markUnregistered() { return registered.compareAndSet(true, false); }

    public boolean isRegistered() { return registered.get(); }

    public RemoteBuffer descriptor() {
        if (!registered.get()) throw new IllegalStateException("buffer is unregistered");
        return new RemoteBuffer(remoteAddress, length, token, generation);
    }
}
