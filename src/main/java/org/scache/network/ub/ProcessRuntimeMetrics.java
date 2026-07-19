package org.scache.network.ub;

final class ProcessRuntimeMetrics {
    final long generation;
    final long initCalls;
    final long uninitCalls;
    final long activeTransports;
    final boolean initialized;

    ProcessRuntimeMetrics(long[] raw) {
        if (raw == null || raw.length != 5) throw new UrmaException("invalid process runtime metrics");
        generation = raw[0];
        initCalls = raw[1];
        uninitCalls = raw[2];
        activeTransports = raw[3];
        initialized = raw[4] != 0;
    }

    @Override public String toString() {
        return "generation=" + generation + " initCalls=" + initCalls
            + " uninitCalls=" + uninitCalls + " activeTransports=" + activeTransports
            + " initialized=" + initialized;
    }
}
