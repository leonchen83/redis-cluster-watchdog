package com.moilioncircle.replicator.cluster.util.net;

public class ServiceOverloadException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ServiceOverloadException() {
        super((String) null);
    }

    public ServiceOverloadException(String message) {
        super(message);
    }

    public ServiceOverloadException(String message, Throwable cause) {
        super(message, cause);
    }

    public ServiceOverloadException(Throwable cause) {
        super(cause);
    }

    public ServiceOverloadException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
