package com.hazelblast.api.exceptions;

public class RemoteMethodTimeoutException extends RuntimeException {

    public RemoteMethodTimeoutException(String message, Throwable cause) {
        super(message,cause);
    }
}
