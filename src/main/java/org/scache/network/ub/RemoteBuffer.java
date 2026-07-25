package org.scache.network.ub;
public class RemoteBuffer {
    public final long remoteAddress;
    public final long length;
    public final int token;
    /** Provider-specific registration generation (zero for generic backends). */
    public final long generation;

    public RemoteBuffer(long remoteAddress, long length, int token) {
        this(remoteAddress, length, token, 0L);
    }

    public RemoteBuffer(long remoteAddress, long length, int token, long generation) {
        this.remoteAddress = remoteAddress;
        this.length = length;
        this.token = token;
        this.generation = generation;
    }
}
