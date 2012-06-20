package com.hazelblast.server.pojoslice;

import com.hazelblast.server.Slice;
import com.hazelblast.server.SliceLifecycleAware;
import com.hazelblast.server.SliceParameters;
import com.hazelblast.server.SlicePartitionAware;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;

import java.lang.reflect.Field;
import java.util.Map;

import static com.hazelblast.server.pojoslice.PojoUtils.getPublicFields;
import static com.hazelblast.utils.Arguments.notNull;
import static java.lang.String.format;

/**
 * The PojoSlice is a {@link com.hazelblast.server.Slice} that contains a single Pojo and all public
 * fields on this Pojo will be exposed as a service.
 * <p/>
 * If the Pojo implements {@link SliceLifecycleAware} it will get callbacks for lifecycle events of the Slice.
 * <p/>
 * If the Pojo implements {@link HazelcastInstanceAware} it will get a callback when the {@link #onStart()} is called
 * with the {@link HazelcastInstance} to be used.
 *
 * If the Pojo implements {@link SlicePartitionAware} it will get callbacks when partitions are added/removed
 * from the {@link Slice}.
 *
 * @author Peter Veentjer.
 */
public final class PojoSlice implements Slice {

    private final Object target;
    private final Map<String, Field> services;
    private final SliceParameters sliceParameters;

    public PojoSlice(Object target) {
        this(target, DEFAULT_NAME, Hazelcast.getDefaultInstance());
    }

    public PojoSlice(Object target, HazelcastInstance hazelcastInstance) {
        this(target, DEFAULT_NAME, hazelcastInstance);
    }

    public PojoSlice(Object target, String sliceName, HazelcastInstance hazelcastInstance) {
        this(target, new SliceParameters(hazelcastInstance, sliceName));
    }

    /**
     * Creates a PojoSlice.
     *
     * @param target the object this PojoSlice wraps.
     * @throws NullPointerException if target is null.
     */
    public PojoSlice(Object target, SliceParameters sliceParameters) {
        this.target = notNull("target", target);
        Class targetClass = target.getClass();
        this.services = getPublicFields(targetClass);
        this.sliceParameters = sliceParameters;
    }

    public String getName() {
        return sliceParameters.name;
    }

    public HazelcastInstance getHazelcastInstance() {
        return sliceParameters.hazelcastInstance;
    }

    public Object getService(String serviceName) {
        notNull("serviceName", serviceName);

        if (serviceName.isEmpty()) {
            throw new IllegalArgumentException("serviceName can't be empty");
        }

        serviceName = serviceName.substring(0, 1).toLowerCase() + serviceName.substring(1);

        Field field = services.get(serviceName);
        if (field == null) {
            throw new IllegalArgumentException(format("Unknown service [%s], it is not found as field on class " + target.getClass(), serviceName));
        }

        try {
            return field.get(target);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(format("Inaccessible field [%s]", serviceName), e);
        }
    }

    public void onPartitionAdded(int partitionId) {
        if (target instanceof SlicePartitionAware) {
            ((SlicePartitionAware) target).onPartitionAdded(partitionId);
        }
    }

    public void onPartitionRemoved(int partitionId) {
        if (target instanceof SlicePartitionAware) {
            ((SlicePartitionAware) target).onPartitionRemoved(partitionId);
        }
    }

    public void onStart() {
        if (target instanceof SliceLifecycleAware) {
            ((SliceLifecycleAware) target).onStart();
        }

        if (target instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) target).setHazelcastInstance(sliceParameters.hazelcastInstance);
        }
    }

    public void onStop() {
        if (target instanceof SliceLifecycleAware) {
            ((SliceLifecycleAware) target).onStop();
        }
    }
}
