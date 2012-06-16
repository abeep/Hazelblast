package com.hazelblast.client.basic;

import com.hazelblast.api.LoadBalancer;
import com.hazelblast.client.basic.MethodType;

import java.lang.reflect.Method;

class RemoteMethodInfo {
    final Method method;
    final MethodType methodType;
    final int partitionKeyIndex;
    final Method partitionKeyProperty;
    final String[] argTypes;
    final long timeoutMs;
    final LoadBalancer loadBalancer;

    RemoteMethodInfo(Method method, MethodType methodType, int partitionKeyIndex,
                     Method partitionKeyProperty, long timeoutMs, LoadBalancer loadBalancer) {
        this.method = method;
        this.methodType = methodType;
        this.partitionKeyIndex = partitionKeyIndex;
        this.partitionKeyProperty = partitionKeyProperty;
        this.timeoutMs = timeoutMs;
        this.loadBalancer = loadBalancer;

        Class[] parameterTypes = method.getParameterTypes();
        argTypes = new String[parameterTypes.length];
        for (int k = 0; k < argTypes.length; k++) {
            argTypes[k] = parameterTypes[k].getName();
        }
    }
}
