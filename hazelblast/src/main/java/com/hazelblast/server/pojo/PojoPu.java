package com.hazelblast.server.pojo;

import com.hazelblast.api.ProcessingUnit;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

/**
 * The PojoPu is a {@link ProcessingUnit} that contains a single Pojo and all public fields on this Pojo will be
 * exposed as a service.
 * <p/>
 * <h2>onStart</h2>
 * If the pojo exposes a method 'public void onStart()' it will be called when this ProcessingUnit it started.
 * <p/>
 * <h2>onStop</h2>
 * If the pojo exposes a method 'public void onStop()' it will be called when this ProcessingUnit is stopped.
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
public class PojoPu implements ProcessingUnit {

    private final Object target;
    private final Method onPartitionAddedMethod;
    private final Method onPartitionRemovedMethod;
    private final Method onStopMethod;
    private final Method onStartMethod;
    private final Map<String,Field> services;

    /**
     * Creates a PojoPu.
     *
     * @param target the object this PojoPu wraps.
     * @throws NullPointerException if target is null.
     */
    public PojoPu(Object target) {
        if (target == null) {
            throw new NullPointerException("target can't be null");
        }
        this.target = target;

        Class targetClass = target.getClass();

        this.onPartitionAddedMethod = PojoUtils.getPublicVoidMethod(targetClass, "onPartitionAdded", Integer.TYPE);
        this.onPartitionRemovedMethod = PojoUtils.getPublicVoidMethod(targetClass, "onPartitionRemoved", Integer.TYPE);
        this.onStartMethod = PojoUtils.getPublicVoidMethod(targetClass, "onStart");
        this.onStopMethod = PojoUtils.getPublicVoidMethod(targetClass, "onStop");
        this.services = PojoUtils.getPublicFields(targetClass);
    }

    public Object getService(String serviceName) {
        if (serviceName == null) {
            throw new NullPointerException("serviceName can't be null");
        }

        if (serviceName.isEmpty()) {
            throw new IllegalArgumentException("serviceName can't be empty");
        }

        serviceName = serviceName.substring(0, 1).toLowerCase() + serviceName.substring(1);

        Field field = services.get(serviceName);
        if(field == null){
            throw new IllegalArgumentException(format("Unknown service [%s], it is not found as field on class "+target.getClass(), serviceName));
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
