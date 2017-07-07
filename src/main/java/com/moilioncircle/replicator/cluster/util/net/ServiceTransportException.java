package com.moilioncircle.replicator.cluster.util.net;

public class ServiceTransportException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ServiceTransportException() {
        super((String) null);
    }

    public ServiceTransportException(String message) {
        super(message);
    }

    public ServiceTransportException(String message, Throwable cause) {
        super(message, cause);
    }

    public ServiceTransportException(Throwable cause) {
        super(cause);
    }

    public ServiceTransportException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
