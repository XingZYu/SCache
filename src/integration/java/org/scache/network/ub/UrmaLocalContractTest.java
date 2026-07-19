package org.scache.network.ub;

import java.nio.ByteBuffer;

/** Provider-local negative contract checks; never starts SCache or Spark. */
public final class UrmaLocalContractTest {
    private static int passed;

    private static void expect(String name, Runnable operation) {
        try {
            operation.run();
            throw new AssertionError(name + " unexpectedly succeeded");
        } catch (RuntimeException expected) {
            passed++;
            System.out.println("PASS " + name + " type=" + expected.getClass().getSimpleName()
                + " root=" + expected.getMessage());
        }
    }

    public static void main(String[] args) {
        System.out.println("TRANSPORT_DIAGNOSTIC_ONLY=true");
        System.out.println("SCACHE_COMPONENTS_STARTED=0");
        System.out.println("SPARK_COMPONENTS_STARTED=0");
        expect("heap_buffer", () -> UrmaTransport.checkDirect(ByteBuffer.allocate(8)));
        expect("negative_offset", () -> UrmaTransport.checkBounds(ByteBuffer.allocateDirect(8), -1, 1));
        expect("negative_length", () -> UrmaTransport.checkBounds(ByteBuffer.allocateDirect(8), 0, -1));
        expect("overflow_bounds", () -> UrmaTransport.checkBounds(ByteBuffer.allocateDirect(8), 7, Integer.MAX_VALUE));
        expect("endpoint_short", () -> EndpointDescriptor.fromBytes(new byte[63]));

        UrmaTransport transport = UrmaTransport.open("openurma0", 16, 4096, true, "listen");
        expect("second_active_transport",
            () -> UrmaTransport.open("openurma0", 16, 4096, true, "listen"));
        expect("zero_capacity_registration",
            () -> transport.registerBuffer(ByteBuffer.allocateDirect(0)));
        UrmaTransport.RegisteredBuffer region = transport.registerBuffer(ByteBuffer.allocateDirect(4096));
        UrmaTransport.RegisteredBuffer region2 = transport.registerBuffer(ByteBuffer.allocateDirect(8192));
        if (region.remoteAddress == region2.remoteAddress) throw new AssertionError("multi-region descriptor collision");
        transport.unregisterBuffer(region2);
        transport.unregisterBuffer(region);
        expect("double_unregister", () -> transport.unregisterBuffer(region));
        expect("use_after_unregister", region::descriptor);
        expect("unknown_request", () -> transport.waitFor(987654321L, 10));
        transport.close();
        transport.close();
        expect("use_after_close", transport::getLocalEndpoint);
        TransportMetrics metrics = transport.closeMetrics();
        if (metrics.activeRegions != 0 || metrics.activeImports != 0 ||
            metrics.activeTransports != 0 || metrics.inflightRequests != 0) {
            throw new AssertionError("resources not zero after close: " + metrics);
        }
        System.out.println("PASS close_resources " + metrics);
        UrmaTransport reopened = UrmaTransport.open("openurma0", 16, 4096, true, "listen");
        expect("cross_transport_owner", () -> reopened.unregisterBuffer(region));
        reopened.close();
        ProcessRuntimeMetrics beforeShutdown = UrmaTransport.processRuntimeMetrics();
        if (beforeShutdown.generation != 2 || beforeShutdown.initCalls != 1 ||
            beforeShutdown.activeTransports != 0 || !beforeShutdown.initialized) {
            throw new AssertionError("same-process reopen runtime mismatch: " + beforeShutdown);
        }
        System.out.println("PASS same_process_reopen " + beforeShutdown);
        UrmaTransport.shutdownProcessRuntime();
        ProcessRuntimeMetrics afterShutdown = UrmaTransport.processRuntimeMetrics();
        if (afterShutdown.uninitCalls != 1 || afterShutdown.initialized) {
            throw new AssertionError("runtime did not shutdown: " + afterShutdown);
        }
        System.out.println("PASS process_runtime_shutdown " + afterShutdown);
        System.out.println("passed=" + (passed + 3));
        System.out.println("result=PASS");
    }
}
