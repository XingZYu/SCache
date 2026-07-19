package org.scache.network.ub;

import java.nio.ByteBuffer;

/** Repeated registration lifecycle on one transport. */
public final class UrmaRegistrationLifecycleTest {
    public static void main(String[] args) {
        int cycles = args.length == 0 ? 100 : Integer.parseInt(args[0]);
        System.out.println("TRANSPORT_DIAGNOSTIC_ONLY=true");
        System.out.println("SCACHE_COMPONENTS_STARTED=0");
        System.out.println("SPARK_COMPONENTS_STARTED=0");
        UrmaTransport transport = UrmaTransport.open("openurma0", 16, 4096, true, "listen");
        for (int cycle = 1; cycle <= cycles; cycle++) {
            UrmaTransport.RegisteredBuffer region =
                transport.registerBuffer(ByteBuffer.allocateDirect(4096 + cycle));
            transport.unregisterBuffer(region);
            TransportMetrics metrics = transport.getMetrics();
            if (metrics.activeRegions != 0 || metrics.providerOutstandingRequests != 0) {
                throw new AssertionError("registration resources not zero cycle=" + cycle + " " + metrics);
            }
            if (cycle == 1 || cycle % 10 == 0 || cycle == cycles) {
                System.out.println("REGISTRATION cycle=" + cycle + " registered="
                    + metrics.registeredRegions + " unregistered=" + metrics.unregisteredRegions
                    + " activeRegions=" + metrics.activeRegions + " result=PASS");
            }
        }
        transport.close();
        TransportMetrics closed = transport.closeMetrics();
        if (closed.registeredRegions != cycles || closed.unregisteredRegions != cycles ||
            closed.activeRegions != 0 || closed.activeImports != 0 ||
            closed.providerOutstandingRequests != 0) {
            throw new AssertionError("final registration metrics mismatch " + closed);
        }
        UrmaTransport.shutdownProcessRuntime();
        System.out.println("CLOSE_METRICS " + closed);
        System.out.println("PROCESS_RUNTIME_FINAL " + UrmaTransport.processRuntimeMetrics());
        System.out.println("registrationCycles=" + cycles);
        System.out.println("result=PASS");
    }
}
