package com.hazelblast.client.basic;

public interface MethodInvocationHandler {

    Object invoke(Object proxy, Object[] args) throws Throwable;
}
