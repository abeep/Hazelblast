package com.hazelblast.client.smarter;

import com.hazelcast.core.HazelcastInstance;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;

public abstract class ProxyMethodHandler {

    protected SmarterProxyProvider  proxyProvider;
    protected ExecutorService executor;
    protected HazelcastInstance hazelcastInstance;

    public void setProxyProvider(SmarterProxyProvider proxyProvider){
        this.proxyProvider = proxyProvider;
        this.executor = proxyProvider.executorService;
        this.hazelcastInstance = proxyProvider.hazelcastInstance;
    }

    public abstract ProxyMethod analyze(Method method);

    public abstract Object invoke(Object proxy, ProxyMethod proxyMethod, Object[] args) throws Throwable;

    public abstract Class<? extends Annotation> getAnnotation();

    protected void fixStackTrace(Throwable cause, StackTraceElement[] clientSideStackTrace) {
        StackTraceElement[] serverSideStackTrace = cause.getStackTrace();
        StackTraceElement[] newStackTrace = new StackTraceElement[clientSideStackTrace.length + serverSideStackTrace.length];
        System.arraycopy(serverSideStackTrace, 0, newStackTrace, 0, serverSideStackTrace.length);
        newStackTrace[serverSideStackTrace.length] = new StackTraceElement("------End remote and begin local stracktrace ------", "", null, -1);
        System.arraycopy(clientSideStackTrace, 1, newStackTrace, serverSideStackTrace.length + 1, clientSideStackTrace.length - 1);
        cause.setStackTrace(newStackTrace);
    }
}
