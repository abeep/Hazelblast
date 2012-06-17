package com.hazelblast.api.exceptions;

/**
 * A {@link RemotingException} that indicates that no members are available.
 *
 * @author Peter Veentjer.
 */
public class NoMemberAvailableException extends RemotingException{
    static final long serialVersionUID = 1;

    public NoMemberAvailableException(String message) {
        super(message);
    }

    public NoMemberAvailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
