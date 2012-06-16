package com.hazelblast.client.basic;

import com.hazelblast.api.exceptions.RemoteMethodTimeoutException;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.concurrent.*;

import static com.hazelblast.utils.Arguments.notNull;
import static java.lang.String.format;

public class LoadBalancedMethodInvocationHandler implements MethodInvocationHandler {
    private final static ILogger logger = Logger.getLogger(LoadBalancedMethodInvocationHandler.class.getName());

    private final RemoteInterfaceInfo remoteInterfaceInfo;
    private final RemoteMethodInfo remoteMethodInfo;
    private final ExecutorService executorService;
    private volatile RemoteMethodInvocationFactory remoteMethodInvocationFactory;
    private final String serviceContextName;

    public LoadBalancedMethodInvocationHandler(RemoteInterfaceInfo remoteInterfaceInfo, RemoteMethodInfo remoteMethodInfo, ExecutorService executorService, String serviceContextName) {
        this.remoteInterfaceInfo = remoteInterfaceInfo;
        this.remoteMethodInfo = notNull("remoteMethodInfo", remoteMethodInfo);
        this.executorService = notNull("executorService", executorService);
        this.serviceContextName = serviceContextName;
    }

    public boolean isLocal() {
        return false;
    }

    public Object invoke(Object proxy, Object[] args) throws Throwable {
        Callable callable = remoteMethodInvocationFactory.create(
                serviceContextName, remoteInterfaceInfo.targetInterface.getSimpleName(), remoteMethodInfo.method.getName(),
                args, remoteMethodInfo.argTypes, null);

        Member targetMember = remoteMethodInfo.loadBalancer.getNext();

        DistributedTask<String> task = new DistributedTask<String>(callable, targetMember);

        Future future = executorService.submit(task);
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
}
