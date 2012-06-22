package com.hazelblast.client;

import com.hazelblast.server.exceptions.PartitionMovedException;
import com.hazelblast.server.SliceServer;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.logging.ILogger;

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


    public final static SerializableRemoteMethodInvocationFactory INSTANCE = new SerializableRemoteMethodInvocationFactory();

    public <T> Callable<T> create(String sliceName, String serviceName, String methodName, Object[] args, String[] argTypes, Object partitionKey) {
        return new RemoteMethodInvocation(sliceName, serviceName, methodName, args, argTypes, partitionKey);
    }

    protected static class RemoteMethodInvocation implements Callable, PartitionAware, Serializable, HazelcastInstanceAware {

        private transient ILogger logger;

        static final long serialVersionUID = 1;

        private final String sliceName;
        private final String serviceName;
        private final String methodName;
        private final Object[] args;
        private final Object partitionKey;
        private final String[] argTypes;
        private volatile transient HazelcastInstance hazelcastInstance;

        RemoteMethodInvocation(String sliceName, String serviceName, String methodName, Object[] args, String[] argTypes, Object partitionKey) {
            this.sliceName = sliceName;
            this.serviceName = serviceName;
            this.methodName = methodName;
            this.args = args;
            this.partitionKey = partitionKey;
            this.argTypes = argTypes;
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
            this.logger = hazelcastInstance.getLoggingService().getLogger(RemoteMethodInvocation.class.getName());
        }

        public Object call() throws Exception {
            if (logger.isLoggable(Level.FINE)) {
                //todo: better message.
                logger.log(Level.FINE, format("started %s.%s in Slice %s", serviceName, methodName, serviceName));
            }

            try {
                Object result = SliceServer.executeMethod(hazelcastInstance, sliceName, serviceName, methodName, argTypes, args, partitionKey);
                if (logger.isLoggable(Level.FINE)) {
                    //todo: better message
                    logger.log(Level.FINE, format("finished %s.%s in Slice %s", serviceName, methodName, serviceName));
                }

                return result;
            } catch (PartitionMovedException e) {
                if (logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE, format("failed to call %s.%s in Slice %s", serviceName, methodName, serviceName), e);
                }

                throw e;
            } catch (Exception e) {
                //todo: improved exception, want to include args
                if (logger.isLoggable(Level.SEVERE)) {
                    logger.log(Level.SEVERE, format("failed to call %s.%s in Slice %s", serviceName, methodName, serviceName), e);
                }
                throw e;
            } catch (Throwable e) {
                //todo: improved exception, want to include args
                if (logger.isLoggable(Level.SEVERE)) {
                    logger.log(Level.SEVERE, format("failed to call %s.%s in Slice %s", serviceName, methodName, serviceName), e);
                }

                throw new RuntimeException(e);
            }
        }

        public Object getPartitionKey() {
            return partitionKey;
        }
    }
}
