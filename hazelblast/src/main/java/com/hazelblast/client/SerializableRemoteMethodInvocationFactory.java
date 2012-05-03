package com.hazelblast.client;

import com.hazelblast.server.ServiceContextServer;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.logging.Level;

import static java.lang.String.format;

/**
 * A RemoteMethodInvocationFactory that generates {@link Callable} implementation that can be serialized using
 * the java Serialization mechanism.
 *
 * @author Peter Veentjer.
 */
public final class SerializableRemoteMethodInvocationFactory implements RemoteMethodInvocationFactory {

    private final static ILogger logger = Logger.getLogger(SerializableRemoteMethodInvocationFactory.class.getName());

    public final static SerializableRemoteMethodInvocationFactory INSTANCE = new SerializableRemoteMethodInvocationFactory();

    public <T> Callable<T> create(String serviceContext, String serviceName, String methodName, Object[] args, String[] argTypes, Object partitionKey) {
        return new RemoteMethodInvocation(serviceContext, serviceName, methodName, args, argTypes, partitionKey);
    }

    protected static class RemoteMethodInvocation implements Callable, PartitionAware, Serializable {
        private final String serviceContext;
        private final String serviceName;
        private final String methodName;
        private final Object[] args;
        private transient final Object partitionKey;
        private final String[] argTypes;

        RemoteMethodInvocation(String serviceContext, String serviceName, String methodName, Object[] args, String[] argTypes, Object partitionKey) {
            this.serviceContext = serviceContext;
            this.serviceName = serviceName;
            this.methodName = methodName;
            this.args = args;
            this.partitionKey = partitionKey;
            this.argTypes = argTypes;
        }

        public Object call() throws Exception {
            if (logger.isLoggable(Level.FINE)) {
                //todo: better message.
                logger.log(Level.FINE, format("started %s.%s in serviceContext %s", serviceName, methodName, serviceName));
            }

            try {
                Object result = ServiceContextServer.executeMethod(serviceContext, serviceName, methodName, argTypes,args);
                if (logger.isLoggable(Level.FINE)) {
                    //todo: better message
                    logger.log(Level.FINE, format("finished %s.%s in serviceContext %s", serviceName, methodName, serviceName));
                }

                return result;
            } catch (Exception e) {
                //todo: improved exception, want to include args
                if (logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE, format("failed to call %s.%s in serviceContext %s", serviceName, methodName, serviceName), e);
                }
                throw e;
            } catch (Throwable e) {
                //todo: improved exception, want to include args
                if (logger.isLoggable(Level.SEVERE)) {
                    logger.log(Level.SEVERE, format("failed to call %s.%s in serviceContext %s", serviceName, methodName, serviceName), e);
                }

                throw new RuntimeException(e);
            }
        }

        public Object getPartitionKey() {
            return partitionKey;
        }
    }
}
