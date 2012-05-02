package com.hazelblast.client;

import com.hazelblast.server.ServiceContextServer;
import com.hazelcast.core.PartitionAware;

import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * A RemoteMethodInvocationFactory that generates {@link Callable} implementation that can be serialized using
 * the java Serialization mechanism.
 *
 * @author Peter Veentjer.
 */
public final class SerializableRemoteMethodInvocationFactory implements RemoteMethodInvocationFactory {

    public final static SerializableRemoteMethodInvocationFactory INSTANCE = new SerializableRemoteMethodInvocationFactory();

    public <T> Callable<T> create(String serviceContext, String serviceName, String methodName, Object[] args, Object partitionKey) {
        return new RemoteMethodInvocation(serviceContext, serviceName, methodName, args, partitionKey);
    }

    protected static class RemoteMethodInvocation implements Callable, PartitionAware, Serializable {
        private final String serviceContext;
        private final String serviceName;
        private final String methodName;
        private final Object[] args;
        private transient final Object partitionKey;

        RemoteMethodInvocation(String serviceContext, String serviceName, String methodName, Object[] args, Object partitionKey) {
            this.serviceContext = serviceContext;
            this.serviceName = serviceName;
            this.methodName = methodName;
            this.args = args;
            this.partitionKey = partitionKey;
        }

        public Object call() throws Exception {
            return ServiceContextServer.executeMethod(serviceContext, serviceName, methodName, args);
        }

        public Object getPartitionKey() {
            return partitionKey;
        }
    }
}
