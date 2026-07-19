package org.scache.network.ub;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Simple URMA server — TCP control channel only for endpoint exchange.
 * URMA data (WRITE/READ/SEND) goes directly to the server's registered buffer.
 * Matches the proven native C pattern: one session, exchange endpoints, then wait.
 */
public class UrmaServer {
    public static void main(String[] args) throws Exception {
        System.out.println("TRANSPORT_DIAGNOSTIC_ONLY=true");
        System.out.println("SCACHE_COMPONENTS_STARTED=0");
        System.out.println("SPARK_COMPONENTS_STARTED=0");
        int port = 19090;
        String device = "openurma0";
        int bufferBytes = 1024 * 1024;
        int maxChunk = 4096;
        boolean exitAfterConnect = false;
        boolean multiRegion = false;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--control-port": port = Integer.parseInt(args[++i]); break;
                case "--device": device = args[++i]; break;
                case "--buffer-bytes": bufferBytes = Integer.parseInt(args[++i]); break;
                case "--max-chunk-bytes": maxChunk = Integer.parseInt(args[++i]); break;
                case "--exit-after-connect": exitAfterConnect = true; break;
                case "--multi-region": multiRegion = true; break;
            }
        }
        log("Starting on " + device + " port=" + port + " buffer=" + bufferBytes);

        ByteBuffer buffer = ByteBuffer.allocateDirect(bufferBytes);
        log("Direct buffer: " + bufferBytes + " bytes isDirect=" + buffer.isDirect());
        UrmaTransport transport = UrmaTransport.open(device, 128, maxChunk, false);
        log("Transport initialized");
        UrmaTransport.RegisteredBuffer localRegion = transport.registerBuffer(buffer);
        log("Buffer registered handle=" + localRegion.handle + " address="
            + localRegion.remoteAddress + " length=" + localRegion.length
            + " token=" + localRegion.token);
        ByteBuffer secondaryBuffer = null;
        UrmaTransport.RegisteredBuffer secondaryRegion = null;
        if (multiRegion) {
            secondaryBuffer = ByteBuffer.allocateDirect(bufferBytes);
            secondaryRegion = transport.registerBuffer(secondaryBuffer);
            log("Secondary buffer registered handle=" + secondaryRegion.handle + " address="
                + secondaryRegion.remoteAddress + " length=" + secondaryRegion.length
                + " token=" + secondaryRegion.token);
        }
        EndpointDescriptor localEp = transport.getLocalEndpoint();
        log("Local: " + localEp);

        try (ServerSocket ss = new ServerSocket(port, 5, InetAddress.getLoopbackAddress())) {
            while (true) {
                log("Accepting on port " + port);
                Socket sock = ss.accept();
                log("Client: " + sock.getRemoteSocketAddress());

                DataInputStream in = new DataInputStream(new BufferedInputStream(sock.getInputStream()));
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));

                // Exchange endpoints (TCP control only)
                byte[] lb = localEp.toBytes();
                out.writeInt(lb.length); out.write(lb); out.flush();
                int el = in.readInt();
                byte[] eb = new byte[el]; in.readFully(eb);
                EndpointDescriptor remoteEp = EndpointDescriptor.fromBytes(eb);
                log("Remote: " + remoteEp);

                transport.connect(remoteEp);
                log("Connected. Waiting for URMA data...");
                out.writeBoolean(true);
                if (multiRegion) {
                    out.writeLong(localRegion.remoteAddress);
                    out.writeLong(localRegion.length);
                    out.writeInt((int) localRegion.token);
                }
                out.flush();
                if (exitAfterConnect) {
                    log("FAULT_INJECTION peer_exit_after_connect exit=23");
                    Runtime.getRuntime().halt(23);
                }

                // Metadata-only verification protocol. No block bytes use TCP.
                while (true) {
                    int command = in.readInt();
                    if (command == 0) break;
                    if (command == 2) {
                        int length = in.readInt();
                        int seed = in.readInt();
                        if (length < 0 || length > buffer.capacity()) {
                            throw new IOException("invalid producer prepare length");
                        }
                        UrmaClient.fillPattern(buffer, length, seed);
                        long crc = UrmaClient.crc(buffer, length);
                        out.writeLong(crc);
                        out.writeBoolean(true);
                        out.flush();
                        log("PRODUCER_READY seed=" + seed + " length=" + length
                            + " expectedCRC=" + crc + " owner=server");
                        continue;
                    }
                    if (command == 3) {
                        if (!multiRegion || secondaryBuffer == null) {
                            throw new IOException("multi-region verification was not enabled");
                        }
                        int firstLength = in.readInt();
                        long firstExpected = in.readLong();
                        int secondLength = in.readInt();
                        long secondExpected = in.readLong();
                        long firstActual = UrmaClient.crc(buffer, firstLength);
                        long secondActual = UrmaClient.crc(secondaryBuffer, secondLength);
                        boolean match = firstActual == firstExpected && secondActual == secondExpected;
                        out.writeBoolean(match);
                        out.flush();
                        log("MULTI_REGION_VERIFY firstHandle=" + localRegion.handle
                            + " secondHandle=" + secondaryRegion.handle
                            + " firstCRC=" + firstActual + " secondCRC=" + secondActual
                            + " result=" + (match ? "PASS" : "FAIL"));
                        continue;
                    }
                    if (command != 1) throw new IOException("unknown control command " + command);
                    int length = in.readInt();
                    long expectedCrc = in.readLong();
                    if (length < 0 || length > buffer.capacity()) throw new IOException("invalid verify length");
                    byte[] verify = new byte[length];
                    buffer.position(0);
                    buffer.get(verify);
                    CRC32 crc = new CRC32();
                    crc.update(verify);
                    boolean match = crc.getValue() == expectedCrc;
                    out.writeBoolean(match);
                    out.flush();
                    log("VERIFY length=" + length + " expectedCRC=" + expectedCrc
                        + " actualCRC=" + crc.getValue() + " result=" + (match ? "PASS" : "FAIL"));
                }

                // Show first bytes of buffer for verification
                byte[] peek = new byte[64];
                buffer.clear();
                int peekLength = Math.min(64, buffer.capacity());
                buffer.get(peek, 0, peekLength);
                log("Buffer preview: " + bytesToHex(peek, peekLength));

                sock.close();
                log("Session done");
                // Break single-session server for now
                break;
            }
        } finally {
            try {
                transport.close();
                log("CLOSE_METRICS " + transport.closeMetrics());
            } finally {
                UrmaTransport.shutdownProcessRuntime();
                log("PROCESS_RUNTIME " + UrmaTransport.processRuntimeMetrics());
                log("Shutdown");
            }
        }
    }
    static void log(String m) { System.out.println("[urma-server] " + m); }
    static String bytesToHex(byte[] d, int max) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Math.min(d.length, max); i++)
            sb.append(String.format("%02x ", d[i] & 0xFF));
        return sb.toString();
    }
}
