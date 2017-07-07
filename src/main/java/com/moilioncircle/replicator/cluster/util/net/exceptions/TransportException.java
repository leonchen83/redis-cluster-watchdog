package com.moilioncircle.replicator.cluster.util.net.exceptions;

public class TransportException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public TransportException() {
        super((String) null);
    }

    public TransportException(String message) {
        super(message);
    }

    public TransportException(String message, Throwable cause) {
        super(message, cause);
    }

    public TransportException(Throwable cause) {
        super(cause);
    }

    public TransportException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
