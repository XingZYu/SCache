package org.scache.network.ub;
public class TransportMetrics {
    public final long submittedRequests, completedRequests, failedRequests, timedOutRequests;
    public final long writeBytes, readBytes, sendBytes;
    public final long checksumErrors, chunkedBlocks, chunks;
    public final long inflightRequests, submittedBytes, completedBytes, lateCompletions, unknownWaits;
    public final long writeRequests, readRequests, sendRequests, logicalBlocks;
    public final long registeredRegions, unregisteredRegions, activeRegions;
    public final long importedRegions, unimportedRegions, activeImports;
    public final long initCalls, uninitCalls, activeTransports, closeErrors, tcpPayloadBytes;
    public final long providerOutstandingRequests, providerDrainedRequests;
    TransportMetrics(long[] raw) {
        if (raw == null || raw.length != 32) throw new UrmaException("invalid native metrics vector");
        int i = 0;
        submittedRequests = raw[i++]; completedRequests = raw[i++];
        failedRequests = raw[i++];    timedOutRequests = raw[i++];
        writeBytes = raw[i++];        readBytes = raw[i++];
        sendBytes = raw[i++];         checksumErrors = raw[i++];
        chunkedBlocks = raw[i++];     chunks = raw[i++];
        inflightRequests = raw[i++];  submittedBytes = raw[i++];
        completedBytes = raw[i++];    lateCompletions = raw[i++];
        unknownWaits = raw[i++];      writeRequests = raw[i++];
        readRequests = raw[i++];      sendRequests = raw[i++];
        logicalBlocks = raw[i++];     registeredRegions = raw[i++];
        unregisteredRegions = raw[i++]; activeRegions = raw[i++];
        importedRegions = raw[i++];   unimportedRegions = raw[i++];
        activeImports = raw[i++];      initCalls = raw[i++];
        uninitCalls = raw[i++];        activeTransports = raw[i++];
        closeErrors = raw[i++];        tcpPayloadBytes = raw[i++];
        providerOutstandingRequests = raw[i++]; providerDrainedRequests = raw[i++];
    }
    @Override public String toString() {
        return String.format("submitted=%d completed=%d failed=%d timeout=%d "
            + "inflight=%d submittedB=%d completedB=%d writeB=%d readB=%d sendB=%d "
            + "providerOutstanding=%d providerDrained=%d chkErr=%d logicalBlocks=%d chunks=%d "
            + "activeRegions=%d activeImports=%d activeTransports=%d tcpPayloadB=%d",
            submittedRequests, completedRequests, failedRequests, timedOutRequests,
            inflightRequests, submittedBytes, completedBytes, writeBytes, readBytes, sendBytes,
            providerOutstandingRequests, providerDrainedRequests, checksumErrors, logicalBlocks, chunks,
            activeRegions, activeImports, activeTransports, tcpPayloadBytes);
    }
}
