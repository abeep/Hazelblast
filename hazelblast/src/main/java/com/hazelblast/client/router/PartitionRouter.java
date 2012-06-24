package com.hazelblast.client.router;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static com.hazelblast.utils.Arguments.notNull;
import static java.lang.String.format;

public class PartitionRouter implements Router {

    private final ILogger logger;

    private final Method propertyMethod;
    private final Field propertyField;
    private final int partitionKeyArgIndex;
    private final PartitionService partitionService;

    public PartitionRouter(HazelcastInstance hazelcastInstance, Method propertyMethod, Field propertyField, int partitionKeyArgIndex) {
        notNull("hazelcastInstance", hazelcastInstance);
        this.logger = hazelcastInstance.getLoggingService().getLogger(RoundRobinLoadBalancer.class.getName());
        this.partitionService = hazelcastInstance.getPartitionService();
        this.propertyMethod = propertyMethod;
        this.propertyField = propertyField;
        this.partitionKeyArgIndex = partitionKeyArgIndex;
    }

    public Target getTarget(Method method, Object[] args) throws Throwable {
        Object partitionKey = getPartitionKey(method, args);
        Partition partition = partitionService.getPartition(partitionKey);
        Member member = partition.getOwner();
        return new Target(member, partition.getPartitionId());
    }

    private Object getPartitionKey(Method method, Object[] args) throws Throwable {
        Object arg = args[partitionKeyArgIndex];
        if (arg == null) {
            throw new NullPointerException(format("The @PartitionKey argument of partitioned method '%s' can't be null", method));
        }

        Object partitionKey;
        if (propertyMethod != null) {
            try {
                partitionKey = propertyMethod.invoke(arg);
                if (partitionKey == null) {
                    //todo: improve message
                    throw new NullPointerException(
                            format("The @PartitionKey argument of type '%s' used in partitioned method '%s' can't return null for '%s'",
                                    args[partitionKeyArgIndex].getClass().getName(), method, propertyMethod));

                }
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(format("[%s] The @PartitionKey.property method '%s' failed to be invoked",
                        method, propertyMethod), e);
            }
        } else if (propertyField != null) {
            try {
                partitionKey = propertyField.get(arg);

                //todo: improve message
                if (partitionKey == null) {
                    throw new NullPointerException();
                }
            } catch (IllegalAccessException e) {
                //todo: improve message
                throw new NullPointerException();
            }
        } else {
            partitionKey = arg;
        }


        if (partitionKey instanceof PartitionAware) {
            partitionKey = ((PartitionAware) partitionKey).getPartitionKey();
            if (partitionKey == null) {
                //todo:
                throw new NullPointerException();
            }
        }

        return partitionKey;
    }
}
