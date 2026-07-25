package org.scache.network.ub;

import java.util.Objects;

/** Immutable capabilities advertised by the real URMA transport backend. */
public final class UbTransportCapabilities {
    public enum BackendClass { REAL }

    public final String backendName;
    public final BackendClass backendClass;
    public final boolean supportsRead;
    public final boolean supportsWrite;
    public final long maxLogicalOperationBytes;
    public final int maxRawOperationBytes;
    public final int maxQueueDepth;
    public final int requiredAlignmentBytes;
    public final boolean requiresJni;
    public final boolean usesSharedCarrier;

    public UbTransportCapabilities(
            String backendName,
            BackendClass backendClass,
            boolean supportsRead,
            boolean supportsWrite,
            long maxLogicalOperationBytes,
            int maxRawOperationBytes,
            int maxQueueDepth,
            int requiredAlignmentBytes,
            boolean requiresJni,
            boolean usesSharedCarrier) {
        if (backendName == null || backendName.trim().isEmpty()) {
            throw new IllegalArgumentException("backendName must not be empty");
        }
        this.backendName = backendName.trim();
        this.backendClass = Objects.requireNonNull(backendClass, "backendClass");
        if (!supportsRead && !supportsWrite) {
            throw new IllegalArgumentException("a transport must support READ or WRITE");
        }
        if (maxLogicalOperationBytes <= 0) {
            throw new IllegalArgumentException("maxLogicalOperationBytes must be positive");
        }
        if (maxRawOperationBytes <= 0 || maxQueueDepth <= 0 || requiredAlignmentBytes <= 0) {
            throw new IllegalArgumentException("raw limit, queue depth, and alignment must be positive");
        }
        if ((requiredAlignmentBytes & (requiredAlignmentBytes - 1)) != 0) {
            throw new IllegalArgumentException("requiredAlignmentBytes must be a power of two");
        }
        if (maxRawOperationBytes > maxLogicalOperationBytes) {
            throw new IllegalArgumentException("raw operation limit cannot exceed logical limit");
        }
        this.supportsRead = supportsRead;
        this.supportsWrite = supportsWrite;
        this.maxLogicalOperationBytes = maxLogicalOperationBytes;
        this.maxRawOperationBytes = maxRawOperationBytes;
        this.maxQueueDepth = maxQueueDepth;
        this.requiredAlignmentBytes = requiredAlignmentBytes;
        this.requiresJni = requiresJni;
        this.usesSharedCarrier = usesSharedCarrier;
    }

    public boolean isReal() { return backendClass == BackendClass.REAL; }
}
