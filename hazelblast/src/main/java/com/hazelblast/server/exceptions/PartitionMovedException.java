package com.hazelblast.server.exceptions;

import com.hazelblast.client.exceptions.RemotingException;

/**
 * An {@link com.hazelblast.client.exceptions.RemotingException} thrown when a remote invoke method is executed on a machine that doesn't contain
 * the partition it once contained.
 *
 * @author Peter Veentjer.
 */
public class PartitionMovedException extends RemotingException {

    static final long serialVersionUID = 1;

    public PartitionMovedException(String message) {
        super(message);
    }
}
