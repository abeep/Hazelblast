package com.hazelblast.client.basic;

public interface MethodInvocationHandler {

    boolean isLocal();

    Object invoke(Object proxy, Object[] args) throws Throwable;
}
