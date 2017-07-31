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

    public ClusterConfigurationException(Throwable cause) {
        super(cause);
    }

    public ClusterConfigurationException(String message, Throwable c) {
        super(message, c);
    }

    protected ClusterConfigurationException(String message, Throwable c, boolean suppression, boolean writable) {
        super(message, c, suppression, writable);
    }
}
