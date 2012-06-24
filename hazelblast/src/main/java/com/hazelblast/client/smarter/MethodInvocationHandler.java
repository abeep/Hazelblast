package com.hazelblast.client.smarter;

public interface MethodInvocationHandler {

    Object invoke(Object proxy, Object[] args)throws Throwable;
}
