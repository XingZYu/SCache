package org.scache.network.ub;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Endpoint descriptor exchanged via TCP control channel before URMA data transfer.
 * Packed binary format (network byte order), 64 bytes total.
 */
public class EndpointDescriptor {
    public int protocolVersion = 1;
    public int uasid;
    public int jettyId;
    public byte[] eid = new byte[16];
    public long segmentAddress;
    public long segmentLength;
    public int segmentToken;
    public int maxChunkBytes = 4096;
    public int transportMode;

    private static final int SIZE = 64; // 4+4+4+4(pad)+16+8+8+4+4+4+4(pad) ≈ 64

    public byte[] toBytes() {
        validate();
        ByteBuffer bb = ByteBuffer.allocate(SIZE).order(ByteOrder.BIG_ENDIAN);
        bb.putInt(protocolVersion);
        bb.putInt(uasid);
        bb.putInt(jettyId);
        bb.putInt(0); // padding
        bb.put(eid);
        bb.putLong(segmentAddress);
        bb.putLong(segmentLength);
        bb.putInt(segmentToken);
        bb.putInt(maxChunkBytes);
        bb.putInt(transportMode);
        bb.putInt(0); // padding
        return bb.array();
    }

    public static EndpointDescriptor fromBytes(byte[] raw) {
        if (raw == null || raw.length != SIZE) {
            throw new UrmaProtocolException("endpoint descriptor must be exactly " + SIZE + " bytes");
        }
        EndpointDescriptor ep = new EndpointDescriptor();
        ByteBuffer bb = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);
        ep.protocolVersion = bb.getInt();
        ep.uasid = bb.getInt();
        ep.jettyId = bb.getInt();
        bb.getInt(); // padding
        bb.get(ep.eid);
        ep.segmentAddress = bb.getLong();
        ep.segmentLength = bb.getLong();
        ep.segmentToken = bb.getInt();
        ep.maxChunkBytes = bb.getInt();
        ep.transportMode = bb.getInt();
        bb.getInt();
        ep.validate();
        return ep;
    }

    public void validate() {
        if (protocolVersion != 1) throw new UrmaProtocolException("unsupported endpoint protocol " + protocolVersion);
        if (eid == null || eid.length != 16) throw new UrmaProtocolException("EID must be exactly 16 bytes");
        if (segmentAddress < 0 || segmentLength < 0) throw new UrmaProtocolException("negative segment field");
        if (maxChunkBytes <= 0 || maxChunkBytes > 4096)
            throw new UrmaProtocolException("invalid Tier S maxChunkBytes " + maxChunkBytes);
    }

    @Override
    public String toString() {
        return String.format("Endpoint{uasid=%d, jetty=%d, segAddr=0x%x, segLen=%d, chunkBytes=%d}",
            uasid, jettyId, segmentAddress, segmentLength, maxChunkBytes);
    }
}
