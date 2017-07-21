package com.moilioncircle.redis.cluster.watchdog;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterConfigurationException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ClusterConfigurationException() {
        super();
    }

    public ClusterConfigurationException(String message) {
        super(message);
    }

    public ClusterConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClusterConfigurationException(Throwable cause) {
        super(cause);
    }

    protected ClusterConfigurationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
