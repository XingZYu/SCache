package org.scache.network.ub;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.CRC32;

/**
 * URMA client — WRITE/READ/SEND with data verification.
 * TCP for endpoint exchange only. URMA for all data.
 */
public class UrmaClient {
    private static int tcpBytes = 0, submitted = 0, completed = 0;
    private static int timeouts = 0, compErrors = 0, chkErrors = 0;
    private static long urmaBytes = 0;
    private static boolean forceRaw;
    private static long remoteLengthOverride = -1;
    private static int remoteTokenOverride = Integer.MIN_VALUE;
    private static int closeWithInflight;
    private static boolean expectUnregisterBusy;
    private static int requestTimeoutMs = 30000;
    private static boolean multiRemote;

    public static void main(String[] args) throws Exception {
        System.out.println("TRANSPORT_DIAGNOSTIC_ONLY=true");
        System.out.println("SCACHE_COMPONENTS_STARTED=0");
        System.out.println("SPARK_COMPONENTS_STARTED=0");
        String server = "127.0.0.1:19090", device = "openurma1", op = "write";
        int payload = 64, iters = 5, maxChunk = 4096, outstanding = 0;
        String waitOrder = "reverse";
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--server": server = args[++i]; break;
                case "--device": device = args[++i]; break;
                case "--operation": op = args[++i]; break;
                case "--payload-bytes": payload = Integer.parseInt(args[++i]); break;
                case "--iterations": iters = Integer.parseInt(args[++i]); break;
                case "--max-chunk-bytes": maxChunk = Integer.parseInt(args[++i]); break;
                case "--outstanding": outstanding = Integer.parseInt(args[++i]); break;
                case "--wait-order": waitOrder = args[++i]; break;
                case "--force-raw": forceRaw = true; break;
                case "--remote-length": remoteLengthOverride = Long.parseLong(args[++i]); break;
                case "--remote-token": remoteTokenOverride = Integer.parseInt(args[++i]); break;
                case "--close-with-inflight": closeWithInflight = Integer.parseInt(args[++i]); break;
                case "--expect-unregister-busy": expectUnregisterBusy = true; break;
                case "--request-timeout-ms": requestTimeoutMs = Integer.parseInt(args[++i]); break;
                case "--multi-remote": multiRemote = true; break;
            }
        }
        final String configuredDevice = device;
        final int configuredMaxChunk = maxChunk;
        String[] hp = server.split(":"); String host = hp[0]; int port = Integer.parseInt(hp[1]);
        log("Op=" + op + " sz=" + payload + " iters=" + iters + " dev=" + device);

        ByteBuffer buf = ByteBuffer.allocateDirect(Math.max(payload + 4096, 1024 * 1024));
        UrmaTransport t = UrmaTransport.open(device, 128, maxChunk, false);
        UrmaTransport.RegisteredBuffer localRegion = t.registerBuffer(buf);
        System.out.println("LOCAL_REGION handle=" + localRegion.handle + " address="
            + localRegion.remoteAddress + " length=" + localRegion.length
            + " token=" + localRegion.token);
        EndpointDescriptor localEp = t.getLocalEndpoint();

        try (Socket sock = new Socket(InetAddress.getLoopbackAddress(), port)) {
            DataInputStream in = new DataInputStream(new BufferedInputStream(sock.getInputStream()));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));

            // Receive server endpoint
            int el = in.readInt(); tcpBytes += 4;
            byte[] eb = new byte[el]; in.readFully(eb); tcpBytes += el;
            EndpointDescriptor remoteEp = EndpointDescriptor.fromBytes(eb);
            log("Remote: " + remoteEp);

            // Send local endpoint
            byte[] lb = localEp.toBytes();
            out.writeInt(lb.length); out.write(lb); out.flush(); tcpBytes += 4 + lb.length;

            t.connect(remoteEp);
            if (!in.readBoolean()) throw new IOException("server transport did not become ready");
            tcpBytes += 1;
            RemoteBuffer firstRemote = null;
            if (multiRemote) {
                firstRemote = new RemoteBuffer(in.readLong(), in.readLong(), in.readInt());
                tcpBytes += 20;
            }
            long effectiveRemoteLength = remoteLengthOverride >= 0 ? remoteLengthOverride : remoteEp.segmentLength;
            int effectiveRemoteToken = remoteTokenOverride == Integer.MIN_VALUE
                ? remoteEp.segmentToken : remoteTokenOverride;
            RemoteBuffer remote = new RemoteBuffer(remoteEp.segmentAddress, effectiveRemoteLength, effectiveRemoteToken);
            log("Connected. Running tests...");

            if (multiRemote) {
                ByteBuffer secondLocalBuffer = ByteBuffer.allocateDirect(4096);
                UrmaTransport.RegisteredBuffer secondLocal = t.registerBuffer(secondLocalBuffer);
                fillPattern(buf, 64, 0x7111);
                fillPattern(secondLocalBuffer, 64, 0x7222);
                long firstRequest = t.write(firstRemote, buf, 0, 64);
                long secondRequest = t.write(remote, secondLocalBuffer, 0, 64);
                submitted += 2;
                urmaBytes += 128;
                if (t.waitFor(secondRequest, requestTimeoutMs) == 0) completed++;
                if (t.waitFor(firstRequest, requestTimeoutMs) == 0) completed++;
                long firstCrc = crc(buf, 64);
                long secondCrc = crc(secondLocalBuffer, 64);
                t.unregisterBuffer(secondLocal);
                out.writeInt(3);
                out.writeInt(64);
                out.writeLong(firstCrc);
                out.writeInt(64);
                out.writeLong(secondCrc);
                out.flush();
                tcpBytes += 28;
                if (!in.readBoolean()) chkErrors++; else tcpBytes += 1;
                System.out.println("MULTI_REGION localHandles=" + localRegion.handle + ","
                    + secondLocal.handle + " remoteTokens=" + firstRemote.token + ","
                    + remote.token + " requests=" + firstRequest + "," + secondRequest
                    + " firstCRC=" + firstCrc + " secondCRC=" + secondCrc);
                iters = 2;
            } else if (closeWithInflight > 0) {
                if (!op.equals("write") || payload <= 0 || payload > 4096)
                    throw new IllegalArgumentException("close-with-inflight requires raw WRITE size 1..4096");
                fillPattern(buf, payload, 0x6262);
                for (int i = 0; i < closeWithInflight; i++) {
                    t.write(remote, buf, 0, payload);
                    submitted++;
                    urmaBytes += payload;
                }
                iters = closeWithInflight;
            } else if (outstanding > 0) {
                if (!op.equals("write") || payload <= 0 || payload > 4096)
                    throw new IllegalArgumentException("outstanding diagnostic requires raw WRITE size 1..4096");
                fillPattern(buf, payload, 0x5151);
                long[] requests = new long[outstanding];
                for (int i = 0; i < outstanding; i++) {
                    requests[i] = t.write(remote, buf, 0, payload);
                    submitted++;
                    urmaBytes += payload;
                }
                if (expectUnregisterBusy) {
                    try {
                        t.unregisterBuffer(localRegion);
                        throw new IllegalStateException("unregister-with-inflight unexpectedly succeeded");
                    } catch (UrmaException expected) {
                        if (expected.getMessage() == null || !expected.getMessage().contains("ERR_BUSY")) {
                            throw expected;
                        }
                        System.out.println("PASS unregister_with_inflight type="
                            + expected.getClass().getSimpleName() + " root=" + expected.getMessage());
                    }
                }
                System.out.println("submission_order=" + java.util.Arrays.toString(requests));
                List<Integer> order = new ArrayList<>();
                for (int i = 0; i < outstanding; i++) order.add(i);
                if (waitOrder.equals("reverse")) {
                    Collections.reverse(order);
                } else if (waitOrder.equals("shuffle")) {
                    Collections.shuffle(order, new Random(0x5CA1E + outstanding));
                } else if (!waitOrder.equals("parallel")) {
                    throw new IllegalArgumentException("wait-order must be reverse, shuffle, or parallel");
                }
                System.out.println("wait_order_type=" + waitOrder + " indexes=" + order);
                if (waitOrder.equals("parallel")) {
                    ExecutorService executor = Executors.newFixedThreadPool(2);
                    try {
                        List<Future<Boolean>> futures = new ArrayList<>();
                        for (int i = 0; i < outstanding; i++) {
                            final int index = i;
                            futures.add(executor.submit(new Callable<Boolean>() {
                                @Override public Boolean call() {
                                    return t.waitFor(requests[index], requestTimeoutMs) == 0;
                                }
                            }));
                        }
                        for (Future<Boolean> future : futures) if (future.get()) completed++;
                    } finally {
                        executor.shutdownNow();
                    }
                } else {
                    for (int index : order) {
                        if (t.waitFor(requests[index], requestTimeoutMs) == 0) completed++;
                    }
                }
                long expectedCrc = crc(buf, payload);
                out.writeInt(1); out.writeInt(payload); out.writeLong(expectedCrc); out.flush();
                tcpBytes += 16;
                if (!in.readBoolean()) chkErrors++;
                else tcpBytes += 1;
                iters = outstanding;
            } else for (int iter = 0; iter < iters; iter++) {
                log("  Iter " + (iter+1) + "/" + iters);
                boolean ok = false;
                fillPattern(buf, payload, iter);
                switch (op) {
                    case "write": ok = doWrite(t, buf, payload, remote); break;
                    case "read":  ok = doRead(t, buf, payload, remote, in, out, iter); break;
                    case "send":  ok = doSend(t, buf, payload, remote); break;
                }
                if (ok && op.equals("write")) {
                    long expectedCrc = crc(buf, payload);
                    out.writeInt(1); out.writeInt(payload); out.writeLong(expectedCrc); out.flush();
                    tcpBytes += 16;
                    boolean remoteVerified = in.readBoolean(); tcpBytes += 1;
                    if (!remoteVerified) { chkErrors++; ok = false; }
                }
                submitted++;
                if (ok) completed++; else log("    FAILED");
            }

            // Signal done to server
            out.writeInt(0); out.flush(); tcpBytes += 4;
        } finally {
            boolean closedSuccessfully = false;
            try {
                t.close();
                closedSuccessfully = true;
                print(op, payload, iters, t.closeMetrics());
            } finally {
                if (closedSuccessfully) {
                    UrmaTransport.shutdownProcessRuntime();
                    System.out.println("process_runtime=" + UrmaTransport.processRuntimeMetrics());
                } else {
                    try {
                        UrmaTransport.open(configuredDevice, 128, configuredMaxChunk, false);
                        throw new AssertionError(
                            "a new transport opened while the failed-close transport still owns provider work");
                    } catch (UrmaInitializationException expected) {
                        if (expected.getMessage() == null ||
                            !expected.getMessage().contains("ERR_BUSY")) {
                            throw expected;
                        }
                        System.err.println("PASS reopen_after_close_failure_rejected type="
                            + expected.getClass().getSimpleName() + " root=" + expected.getMessage());
                    }
                    System.err.println("process_runtime_shutdown=SKIPPED_ACTIVE_TRANSPORT_AFTER_CLOSE_FAILURE");
                }
            }
        }
    }

    private static boolean doWrite(UrmaTransport t, ByteBuffer buf, int len, RemoteBuffer remote) {
        long reqId = (!forceRaw && (len == 0 || len > 4096))
            ? t.writeChunked(remote, buf, 0, len) : t.write(remote, buf, 0, len);
        System.out.println("REQUEST operation=WRITE logicalBytes=" + len + " requestId=" + reqId
            + " expectedChunks=" + (len == 0 ? 0 : (len + 4095) / 4096));
        urmaBytes += len;
        int s = t.waitFor(reqId, requestTimeoutMs);
        if (s != 0) { if (s == -1) timeouts++; else compErrors++; return false; }
        return true;
    }

    private static boolean doRead(UrmaTransport t, ByteBuffer buf, int len, RemoteBuffer remote,
                                  DataInputStream in, DataOutputStream out, int iteration)
            throws IOException {
        // Metadata-only prepare: the producer owns and fills the registered source.
        int seed = 0x100 + iteration;
        out.writeInt(2);
        out.writeInt(len);
        out.writeInt(seed);
        out.flush();
        tcpBytes += 12;
        long producerCrc = in.readLong();
        boolean producerReady = in.readBoolean();
        tcpBytes += 9;
        if (!producerReady) return false;

        // Consumer never pre-writes the producer region. It only performs READ.
        buf.clear();
        for (int i = 0; i < len; i++) buf.put((byte) 0xA5);
        buf.clear();
        long rReq;
        if (len > 0 && len <= 4096) rReq = t.read(remote, buf, 0, len);
        else rReq = t.readChunked(remote, buf, 0, len);
        System.out.println("REQUEST operation=READ logicalBytes=" + len + " requestId=" + rReq
            + " expectedChunks=" + (len == 0 ? 0 : (len + 4095) / 4096));
        urmaBytes += len;
        int s = t.waitFor(rReq, Math.max(requestTimeoutMs, len > 4096 ? 120000 : 0));
        if (s != 0) { if (s == -1) timeouts++; else compErrors++; return false; }

        // Verify
        buf.position(0);
        for (int i = 0; i < len; i++) {
            byte actual = buf.get();
            byte expected = (byte) ((i * 131 + seed) & 0xFF);
            if (actual != expected) { chkErrors++; return false; }
        }
        long actualCrc = crc(buf, len);
        System.out.println("READ_VERIFY seed=" + seed + " length=" + len
            + " expectedCRC=" + producerCrc + " actualCRC=" + actualCrc);
        if (actualCrc != producerCrc) { chkErrors++; return false; }
        return true;
    }

    private static boolean doSend(UrmaTransport t, ByteBuffer buf, int len, RemoteBuffer remote) {
        fillPattern(buf, len, 0x300);
        long reqId = t.send(buf, 0, len);
        urmaBytes += len;
        int s = t.waitFor(reqId, requestTimeoutMs);
        if (s != 0) { if (s == -1) timeouts++; else compErrors++; return false; }
        return true;
    }

    static void fillPattern(ByteBuffer buf, int len, int seed) {
        buf.clear();
        for (int i = 0; i < len; i++) buf.put((byte) ((i * 131 + seed) & 0xFF));
        buf.flip();
    }
    static long crc(ByteBuffer buf, int len) {
        CRC32 crc = new CRC32();
        ByteBuffer copy = buf.duplicate();
        copy.position(0); copy.limit(len);
        while (copy.hasRemaining()) crc.update(copy.get() & 0xff);
        return crc.getValue();
    }
    static void log(String m) { System.out.println("[urma-client] " + m); }
    static void print(String op, int sz, int iters, TransportMetrics metrics) {
        boolean pass = closeWithInflight > 0
            ? metrics.submittedRequests == closeWithInflight
              && metrics.completedRequests == closeWithInflight && metrics.inflightRequests == 0
              && metrics.failedRequests == 0 && metrics.timedOutRequests == 0
            : submitted == iters && completed == iters && timeouts == 0 && compErrors == 0 && chkErrors == 0;
        System.out.println("\n=== URMA TEST RESULTS ===");
        System.out.println("operation=" + op.toUpperCase());
        System.out.println("payload_bytes=" + sz);
        System.out.println("chunk_bytes=" + Math.min(4096, Math.max(0, sz)));
        System.out.println("chunks=" + (sz == 0 ? 0 : (sz + 4095L) / 4096L));
        System.out.println("iterations=" + iters);
        System.out.println("submitted=" + submitted);
        System.out.println("completions=" + completed);
        System.out.println("timeouts=" + timeouts);
        System.out.println("completion_errors=" + compErrors);
        System.out.println("checksum_errors=" + chkErrors);
        System.out.println("tcp_control_bytes=" + tcpBytes);
        System.out.println("tcp_payload_bytes=0");
        System.out.println("urma_payload_bytes=" + urmaBytes);
        System.out.println("tcp_fallbacks=0");
        System.out.println("native_metrics=" + metrics);
        System.out.println("result=" + (pass ? "PASS" : "FAIL"));
    }
}
