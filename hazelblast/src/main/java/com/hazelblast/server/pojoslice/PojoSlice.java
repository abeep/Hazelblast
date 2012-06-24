package com.hazelblast.server.pojoslice;

import com.hazelblast.server.Slice;
import com.hazelblast.server.SliceConfig;
import com.hazelblast.server.SliceLifecycleListener;
import com.hazelblast.server.SlicePartitionListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.partition.Partition;

import java.lang.reflect.Field;
import java.util.Map;

import static com.hazelblast.server.pojoslice.PojoUtils.getServiceFields;
import static com.hazelblast.utils.Arguments.notNull;
import static java.lang.String.format;

/**
 * The PojoSlice is a {@link com.hazelblast.server.Slice} that contains a single Pojo and all fields that are
 * annotated with {@link Exposed} will be exposed as service.
 * <p/>
 * If the Pojo implements {@link com.hazelblast.server.SliceLifecycleListener} it will get callbacks for lifecycle events of the Slice.
 * <p/>
 * If the Pojo implements {@link HazelcastInstanceAware} it will get a callback when the {@link #onStart()} is called
 * with the {@link HazelcastInstance} to be used.
 * <p/>
 * If the Pojo implements {@link com.hazelblast.server.SlicePartitionListener} it will get callbacks when partitions are added/removed
 * from the {@link Slice}.
 *
 * @author Peter Veentjer.
 */
public final class PojoSlice implements Slice {

    private final Object target;
    private final Map<String, Field> services;
    private final SliceConfig sliceConfig;
    private final HazelcastInstance hazelcastInstance;

    public PojoSlice(Object target) {
        this(target, DEFAULT_NAME);
    }

    public PojoSlice(Object target, String sliceName) {
        this(target, new SliceConfig(sliceName));
    }

    /**
     * Creates a PojoSlice.
     *
     * @param target the object this PojoSlice wraps.
     * @throws NullPointerException if target is null.
     */
    public PojoSlice(Object target, SliceConfig sliceConfig) {
        this.target = notNull("target", target);
        Class targetClass = target.getClass();
        this.services = getServiceFields(targetClass);
        this.sliceConfig = sliceConfig;

        HazelcastInstance instance = null;
        if (target instanceof HazelcastInstanceProvider) {
            instance = ((HazelcastInstanceProvider) target).getHazelcastInstance();
        }

        if (instance == null) {
            instance = Hazelcast.getDefaultInstance();
        }

        this.hazelcastInstance = instance;
    }

    public String getName() {
        return sliceConfig.name;
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
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

    public void onPartitionAdded(Partition partition) {
        if (target instanceof SlicePartitionListener) {
            ((SlicePartitionListener) target).onPartitionAdded(partition);
        }
    }

    public void onPartitionRemoved(Partition partition) {
        if (target instanceof SlicePartitionListener) {
            ((SlicePartitionListener) target).onPartitionRemoved(partition);
        }
    }

    public void onStart() {
        if (target instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) target).setHazelcastInstance(hazelcastInstance);
        }

        if (target instanceof SliceLifecycleListener) {
            ((SliceLifecycleListener) target).onStart();
        }
    }

    public void onStop() {
        if (target instanceof SliceLifecycleListener) {
            ((SliceLifecycleListener) target).onStop();
        }
    }
}
