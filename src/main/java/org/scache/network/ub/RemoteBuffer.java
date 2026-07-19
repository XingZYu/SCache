package org.scache.network.ub;
public class RemoteBuffer {
    public final long remoteAddress;
    public final long length;
    public final int token;
    public RemoteBuffer(long remoteAddress, long length, int token) {
        this.remoteAddress = remoteAddress;
        this.length = length;
        this.token = token;
    }
}
