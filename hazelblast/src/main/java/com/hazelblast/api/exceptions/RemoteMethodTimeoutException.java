package com.hazelblast.api.exceptions;

/**
 * A {@link RuntimeException} thrown when a timeout happens when calling a remote method.
 *
 * @author Peter Veentjer.
 */
public class RemoteMethodTimeoutException extends RuntimeException {

    /**
     * Constructs a RemoteMethodTimeoutException.
     *
     * @param message the message.
     * @param cause  the cause of the exception.
     */
    public RemoteMethodTimeoutException(String message, Throwable cause) {
        super(message,cause);
    }
}
