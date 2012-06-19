package com.hazelblast.server.exceptions;

import com.hazelblast.client.exceptions.RemotingException;

/**
 * A {@link com.hazelblast.client.exceptions.RemotingException} that indicates that no members are available.
 *
 * @author Peter Veentjer.
 */
public class NoMemberAvailableException extends RemotingException {
    static final long serialVersionUID = 1;

    public NoMemberAvailableException(String message) {
        super(message);
    }

    public NoMemberAvailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
