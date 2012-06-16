package com.hazelblast.api.exceptions;

/**
 * A {@link RemotingException} thrown when a timeout happens when calling a remote method.
 *
 * @author Peter Veentjer.
 */
public class RemoteMethodTimeoutException extends RemotingException {

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
