package com.hazelblast.client.impl;

import java.util.concurrent.Callable;

/**
 * A Factory for creating Callable implementations that are executed on the machine where the remotely invoked method
 * is executed. The callable is created in the proxy and serialized to the target machine.
 * <p/>
 * The reason for its existence is that different serialization mechanisms are supported in Hazelcast to transfer
 * all the required data to the correct node.
 *
 * @author Peter Veentjer.
 */
public interface DistributedMethodInvocationFactory {

    /**
     * Creates a Callable that created in the ProxyProvider and send to a remote machine where it is executed.
     *
     * @param sliceName   the name of the Slice to connect to.
     * @param serviceName the name of the service to use.
     * @param methodName  the name of the method
     * @param args        the arguments used to call the method
     * @param partitionId the partition key that determines the correct partition. If the partitionKey is null,
     *                    any node will do.
     * @param <T>
     * @return the Callable.
     * @throws NullPointerException if sliceName, serviceName, methodName or args is null.
     */
    <T> Callable<T> create(String sliceName, String serviceName, String methodName, Object[] args, String[] argTypes, int partitionId);
}
