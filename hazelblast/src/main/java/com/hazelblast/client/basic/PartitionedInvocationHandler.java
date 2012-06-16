package com.hazelblast.client.basic;

import com.hazelblast.api.exceptions.RemoteMethodTimeoutException;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.*;

import static com.hazelblast.utils.Arguments.notNull;
import static java.lang.String.format;

public class PartitionedInvocationHandler extends InvocationHandler {

    private final static ILogger logger = Logger.getLogger(LoadBalancedMethodInvocationHandler.class.getName());

    private final RemoteMethodInfo methodInfo;
    private final ExecutorService executorService;
    private volatile RemoteMethodInvocationFactory remoteMethodInvocationFactory;
    private final String serviceContextName;

    public PartitionedInvocationHandler(RemoteMethodInfo remoteMethodInfo, ExecutorService executorService, String serviceContextName) {
        this.methodInfo = notNull("remoteMethodInfo", remoteMethodInfo);
        this.executorService = notNull("executorService", executorService);
        this.serviceContextName = serviceContextName;
    }

    private Object invoke(RemoteMethodInfo remoteMethodInfo, Object[] args) throws Throwable {
        Object partitionKey = getPartitionKey(remoteMethodInfo, args);

        Callable callable = remoteMethodInvocationFactory.create(
                serviceContextName, remoteInterfaceInfo.targetInterface.getSimpleName(), remoteMethodInfo.method.getName(),
                args, remoteMethodInfo.argTypes, partitionKey);

        Future future = executorService.submit(callable);
        try {
            return future.get(remoteMethodInfo.timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw e.getCause();
        } catch (TimeoutException e) {
            throw new RemoteMethodTimeoutException(
                    format("method '%s' failed to complete in the %s ms",
                            remoteMethodInfo.method.toString(), remoteMethodInfo.timeoutMs), e);
        }
    }

    private Object getPartitionKey(RemoteMethodInfo methodInfo, Object[] args) {
        Object arg = args[methodInfo.partitionKeyIndex];
        if (arg == null) {
            throw new NullPointerException(format("The @PartitionKey argument of partitioned method '%s' can't be null",
                    methodInfo.method));
        }

        Object partitionKey;
        if (methodInfo.partitionKeyProperty == null) {
            if (arg instanceof PartitionAware) {
                partitionKey = ((PartitionAware) arg).getPartitionKey();
                if (partitionKey == null) {
                    throw new NullPointerException(
                            format("The @PartitionKey argument of PartitionAware type '%s' of partitioned method '%s' can't return null for PartitionAware.getPartitionKey()",
                                    args[methodInfo.partitionKeyIndex].getClass().getName(), methodInfo.method));
                }
            } else {
                partitionKey = arg;
            }
        } else {
            try {
                partitionKey = methodInfo.partitionKeyProperty.invoke(arg);

                if (partitionKey == null) {
                    throw new NullPointerException(
                            format("The @PartitionKey argument of PartitionAware type '%s' of partitioned method '%s' can't return null for '%s'",
                                    args[methodInfo.partitionKeyIndex].getClass().getName(), methodInfo.method, methodInfo.partitionKeyProperty));

                }

            } catch (InvocationTargetException e) {
                throw new IllegalArgumentException(format("[%s] The @PartitionKey.property method '%s' failed to be invoked",
                        methodInfo.method, methodInfo.partitionKeyProperty), e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(format("[%s] The @PartitionKey.property method '%s' failed to be invoked",
                        methodInfo.method, methodInfo.partitionKeyProperty), e);
            }
        }

        return partitionKey;
    }
}
