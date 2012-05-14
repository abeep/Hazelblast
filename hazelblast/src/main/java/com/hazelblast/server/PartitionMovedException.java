package com.hazelblast.server;

/**
 * Exception thrown when a remote invoke method is executed on a machine that doesn't contain the partition
 * it once contained.
 *
 * @author Peter Veentjer.
 */
public class PartitionMovedException extends RuntimeException {

    static final long serialVersionUID = 1;

    public PartitionMovedException(String message) {
        super(message);
    }
}
