package org.scache.network.ub;

import java.nio.ByteBuffer;

/** Same-JVM process-runtime lifecycle diagnostic. Never starts SCache or Spark. */
public final class UrmaRuntimeLifecycleTest {
    public static void main(String[] args) {
        int cycles = args.length == 0 ? 100 : Integer.parseInt(args[0]);
        System.out.println("TRANSPORT_DIAGNOSTIC_ONLY=true");
        System.out.println("SCACHE_COMPONENTS_STARTED=0");
        System.out.println("SPARK_COMPONENTS_STARTED=0");
        long initialFd = fdCount();
        long initialThreads = threadCount();
        long initialRss = rssKb();

        for (int cycle = 1; cycle <= cycles; cycle++) {
            UrmaTransport transport = UrmaTransport.open("openurma0", 16, 4096, true, "listen");
            UrmaTransport.RegisteredBuffer first =
                transport.registerBuffer(ByteBuffer.allocateDirect(4096));
            UrmaTransport.RegisteredBuffer second =
                transport.registerBuffer(ByteBuffer.allocateDirect(8192));
            transport.unregisterBuffer(second);
            transport.unregisterBuffer(first);
            transport.close();
            TransportMetrics closed = transport.closeMetrics();
            ProcessRuntimeMetrics runtime = UrmaTransport.processRuntimeMetrics();
            if (closed.activeRegions != 0 || closed.activeImports != 0 ||
                closed.activeTransports != 0 || closed.inflightRequests != 0 ||
                closed.providerOutstandingRequests != 0 || runtime.activeTransports != 0) {
                throw new AssertionError("cycle resources not zero cycle=" + cycle
                    + " close=" + closed + " runtime=" + runtime);
            }
            if (cycle == 1 || cycle % 10 == 0 || cycle == cycles) {
                System.out.println("LIFECYCLE cycle=" + cycle + " fd=" + fdCount()
                    + " threads=" + threadCount() + " rssKb=" + rssKb()
                    + " runtime={" + runtime + "} result=PASS");
            }
        }

        ProcessRuntimeMetrics beforeShutdown = UrmaTransport.processRuntimeMetrics();
        if (beforeShutdown.initCalls != 1 || beforeShutdown.uninitCalls != 0 ||
            beforeShutdown.generation != cycles || beforeShutdown.activeTransports != 0 ||
            !beforeShutdown.initialized) {
            throw new AssertionError("runtime before shutdown mismatch: " + beforeShutdown);
        }
        UrmaTransport.shutdownProcessRuntime();
        ProcessRuntimeMetrics afterShutdown = UrmaTransport.processRuntimeMetrics();
        if (afterShutdown.initCalls != 1 || afterShutdown.uninitCalls != 1 ||
            afterShutdown.activeTransports != 0 || afterShutdown.initialized) {
            throw new AssertionError("runtime shutdown mismatch: " + afterShutdown);
        }
        try {
            UrmaTransport.open("openurma0", 16, 4096, true, "listen");
            throw new AssertionError("open after final runtime shutdown unexpectedly succeeded");
        } catch (UrmaInitializationException expected) {
            System.out.println("PASS open_after_runtime_shutdown type="
                + expected.getClass().getSimpleName() + " root=" + expected.getMessage());
        }
        System.out.println("RESOURCE_DELTA fd=" + (fdCount() - initialFd)
            + " threads=" + (threadCount() - initialThreads)
            + " rssKb=" + (rssKb() - initialRss));
        System.out.println("PROCESS_RUNTIME_FINAL " + afterShutdown);
        System.out.println("sameJvmCycles=" + cycles);
        System.out.println("result=PASS");
    }

    private static long fdCount() {
        String[] entries = new java.io.File("/proc/self/fd").list();
        return entries == null ? -1 : entries.length;
    }

    private static long threadCount() {
        String[] entries = new java.io.File("/proc/self/task").list();
        return entries == null ? -1 : entries.length;
    }

    private static long rssKb() {
        try {
            for (String line : java.nio.file.Files.readAllLines(
                    java.nio.file.Path.of("/proc/self/status"))) {
                if (line.startsWith("VmRSS:")) {
                    return Long.parseLong(line.trim().split("\\s+")[1]);
                }
            }
        } catch (Exception ignored) {
        }
        return -1;
    }
}
