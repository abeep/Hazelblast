package com.hazelblast;

public class PartitionMovedException extends RuntimeException {

    static final long serialVersionUID = 1;

    public PartitionMovedException(String message) {
        super(message);
    }
}
