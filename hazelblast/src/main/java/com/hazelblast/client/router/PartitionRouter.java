package com.hazelblast.client.router;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static com.hazelblast.utils.Arguments.notNull;
import static java.lang.String.format;

/**
 * A {@link Router} that uses partitioning to do the routing. So it inspects some argument of the method call, and
 * based on that argument the right node is selected.
 * <p/>
 * This router is used in combinaton with the {@link com.hazelblast.client.annotations.Partitioned} annotation.
 *
 * @author Peter Veentjer.
 */
public class PartitionRouter implements Router {

    private final Method propertyMethod;
    private final Field propertyField;
    private final int partitionKeyIndex;
    private final PartitionService partitionService;

    public PartitionRouter(HazelcastInstance hazelcastInstance, Method propertyMethod, Field propertyField, int partitionKeyIndex) {
        notNull("hazelcastInstance", hazelcastInstance);
        this.partitionService = hazelcastInstance.getPartitionService();
        this.propertyMethod = propertyMethod;
        this.propertyField = propertyField;
        this.partitionKeyIndex = partitionKeyIndex;
    }

    public Target getTarget(Method method, Object[] args) throws Throwable {
        Object partitionKey = getPartitionKey(method, args);
        Partition partition = partitionService.getPartition(partitionKey);
        Member member = partition.getOwner();
        return new Target(member, partition.getPartitionId());
    }

    private Object getPartitionKey(Method method, Object[] args) throws Throwable {
        Object arg = args[partitionKeyIndex];
        if (arg == null) {
            throw new NullPointerException(format("The partitionkey argument of partitioned method '%s' can't be null", method));
        }

        Object partitionKey;
        if (propertyMethod != null) {
            try {
                partitionKey = propertyMethod.invoke(arg);
                if (partitionKey == null) {
                    throw new NullPointerException(format("Method '%s' that is used to determine the partitionkey, can't return null.", propertyMethod));
                }
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(format("Method '%s' that is used to determine the partitionkey, failed to be invoked.", propertyMethod), e);
            }
        } else if (propertyField != null) {
            try {
                partitionKey = propertyField.get(arg);
                if (partitionKey == null) {
                    throw new NullPointerException(format("Field '%s' that is used to determine the partitionkey, can't return null.", propertyField));
                }
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(format("Field '%s' that is used to determine the partitionkey, failed to be accessed.", propertyField), e);
            }
        } else {
            partitionKey = arg;
        }


        if (partitionKey instanceof PartitionAware) {
            partitionKey = ((PartitionAware) partitionKey).getPartitionKey();
            if (partitionKey == null) {
                throw new IllegalArgumentException(format("PartitionAware class '%s' returned null as partitionkey.", arg.getClass().getName()));
            }
        }

        return partitionKey;
    }
}
