package com.hazelblast.server.pojo;

import com.hazelblast.server.ServiceContext;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import static com.hazelblast.utils.Arguments.notNull;
import static java.lang.String.format;

/**
 * The PojoServiceContext is a {@link com.hazelblast.server.ServiceContext} that contains a single Pojo and all public
 * fields on this Pojo will be exposed as a service.
 * <p/>
 * <h2>start</h2>
 * If the pojo exposes a method 'public void start()' it will be called when this ServiceContext it started.
 * <p/>
 * <h2>stop</h2>
 * If the pojo exposes a method 'public void stop()' it will be called when this ServiceContext is stopped.
 * <p/>
 * <h2>onPartitionAdded</h2>
 * If the pojo exposes a method 'public void onPartitionAdded(int partitionId)' it will be called when a partition
 * is added.
 * <p/>
 * <h2>onPartitionRemoved</h2>
 * If the pojo exposes a method 'public void onPartitionRemoved(int partitionId)' it will be called when a partition
 * is removed,
 *
 * @author Peter Veentjer.
 */
public final class PojoServiceContext implements ServiceContext {

    private final Object target;
    private final Method onPartitionAddedMethod;
    private final Method onPartitionRemovedMethod;
    private final Method onStopMethod;
    private final Method onStartMethod;
    private final Map<String, Field> services;

    /**
     * Creates a PojoServiceContext.
     *
     * @param target the object this PojoServiceContext wraps.
     * @throws NullPointerException if target is null.
     */
    public PojoServiceContext(Object target) {
        this.target = notNull("target", target);

        Class targetClass = target.getClass();

        this.onPartitionAddedMethod = PojoUtils.getPublicVoidMethod(targetClass, "onPartitionAdded", Integer.TYPE);
        this.onPartitionRemovedMethod = PojoUtils.getPublicVoidMethod(targetClass, "onPartitionRemoved", Integer.TYPE);
        this.onStartMethod = PojoUtils.getPublicVoidMethod(targetClass, "start");
        this.onStopMethod = PojoUtils.getPublicVoidMethod(targetClass, "stop");
        this.services = PojoUtils.getPublicFields(targetClass);
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
            throw new IllegalArgumentException(format("Inaccessable field [%s]", serviceName), e);
        }
    }

    public void onPartitionAdded(int partitionId) {
        if (onPartitionAddedMethod == null) {
            return;
        }

        try {
            onPartitionAddedMethod.invoke(target, partitionId);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Failed to call method: " + onPartitionAddedMethod, e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to call method: " + onPartitionAddedMethod, e);
        }
    }

    public void onPartitionRemoved(int partitionId) {
        if (onPartitionRemovedMethod == null) {
            return;
        }

        try {
            onPartitionRemovedMethod.invoke(target, partitionId);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Failed to call method: " + onPartitionRemovedMethod, e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to call method: " + onPartitionRemovedMethod, e);
        }
    }

    public void onStart() {
        if (onStartMethod == null) {
            return;
        }

        try {
            onStartMethod.invoke(target);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Failed to call method: " + onStartMethod, e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to call method: " + onStartMethod, e);
        }
    }

    public void onStop() {
        if (onStopMethod == null) {
            return;
        }

        try {
            onStopMethod.invoke(target);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Failed to call method: " + onStopMethod, e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to call method: " + onStopMethod, e);
        }
    }
}
