package org.scache.network.ub;

import java.util.Locale;

/** Single real-URMA backend selection point below UBBlockTransferService. */
public final class UbTransportFactory {
    private UbTransportFactory() {}

    public static String normalizeBackend(String backend) {
        if (backend == null || backend.trim().isEmpty()) return "real";
        return backend.trim().toLowerCase(Locale.ROOT);
    }

    public static UbTransport open(String backend, String deviceName, int queueDepth,
                                   int maxChunkBytes, boolean strict, String wireRole) {
        String selected = normalizeBackend(backend);
        if (!"real".equals(selected)) {
            throw new IllegalArgumentException(
                "Unsupported production scache.ub.transport=" + selected + "; use real");
        }
        return RealUbTransport.open(deviceName, queueDepth, maxChunkBytes, strict, wireRole);
    }
}
