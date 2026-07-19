package org.scache.network.ub;

public class UrmaTimeoutException extends RuntimeException {
    public UrmaTimeoutException(String msg) { super(msg); }
    public UrmaTimeoutException(String msg, Throwable cause) { super(msg, cause); }
}
