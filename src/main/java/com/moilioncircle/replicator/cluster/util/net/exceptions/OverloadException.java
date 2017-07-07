package com.moilioncircle.replicator.cluster.util.net.exceptions;

public class OverloadException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public OverloadException() {
        super((String) null);
    }

    public OverloadException(String message) {
        super(message);
    }

    public OverloadException(String message, Throwable cause) {
        super(message, cause);
    }

    public OverloadException(Throwable cause) {
        super(cause);
    }

    public OverloadException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
