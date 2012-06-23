package com.hazelblast.client.smarter;

import com.hazelblast.client.router.Router;
import com.hazelcast.core.MemberLeftException;

import java.lang.reflect.Method;

public class ProxyMethod {

    public final Method method;
    public final int partitionKeyIndex;
    public final Method partitionKeyProperty;
    public final String[] argTypes;
    public final long timeoutMs;
    public final Router loadBalancer;
    public final boolean interruptOnTimeout;
    public final ProxyMethodHandler handler;

    public ProxyMethod(Method method, int partitionKeyIndex,
                       Method partitionKeyProperty, long timeoutMs, boolean interruptOnTimeout, Router loadBalancer, ProxyMethodHandler handler) {
        this.method = method;
        this.partitionKeyIndex = partitionKeyIndex;
        this.partitionKeyProperty = partitionKeyProperty;
        this.timeoutMs = timeoutMs;
        this.loadBalancer = loadBalancer;
        this.interruptOnTimeout = interruptOnTimeout;
        this.handler = handler;

        Class[] parameterTypes = method.getParameterTypes();
        argTypes = new String[parameterTypes.length];
        for (int k = 0; k < argTypes.length; k++) {
            argTypes[k] = parameterTypes[k].getName();
        }
    }

    public Object invoke(Object proxy, Object[] args)throws Throwable {
        return handler.invoke(proxy, this, args);
    }
}
