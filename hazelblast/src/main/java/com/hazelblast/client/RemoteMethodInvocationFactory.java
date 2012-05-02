package com.hazelblast.client;

import java.util.concurrent.Callable;

/**
 * A Factory for creating Callable implementations that are executed on the machine where the remotely invoked method
 * is executed. The callable is created in the proxy and serialized to the target machine.
 *
 * The reason for its existence is that different serialization mechanisms are supported in Hazelcast to transfer
 * all the required data to the correct node.
 *
 * @author Peter Veentjer.
 */
public interface RemoteMethodInvocationFactory {

   <T> Callable<T> create(String puName, String serviceName, String methodName, Object[] args, Object partitionKey);
}
