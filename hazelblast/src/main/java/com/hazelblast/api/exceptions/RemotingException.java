package com.hazelblast.api.exceptions;

/**
 * A {@link RuntimeException} that is thrown when a remote method invocation fails caused by the remoting
 * middleware.
 *
 * @author Peter Veentjer.
 */
public class RemotingException extends RuntimeException{

    public RemotingException(String message) {
        super(message);
    }

    public RemotingException(String message, Throwable cause) {
        super(message, cause);
    }
}
