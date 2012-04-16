package com.hazelblast.server.pojo;

import com.hazelblast.api.ProcessingUnit;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static java.lang.String.format;

/**
 * The PojoPu is a {@link ProcessingUnit} that contains a single Pojo and all public fields on this Pojo will be
 * exposed as a service.
 *
 * <h2>onStart</h2>
 * If the pojo exposes a method 'public void onStart()' it will be called when this ProcessingUnit it started.
 *
 * <h2>onStop</h2>
 * If the pojo exposes a method 'public void onStop()' it will be called when this ProcessingUnit is stopped.
 *
 * <h2>onPartitionAdded</h2>
 * If the pojo exposes a method 'public void onPartitionAdded(int partitionId)' it will be called when a partition
 * is added.
 *
 * <h2>onPartitionRemoved</h2>
 * If the pojo exposes a method 'public void onPartitionRemoved(int partitionId)' it will be called when a partition
 * is removed,
 *
 * @author Peter Veentjer.
 */
public class PojoPu implements ProcessingUnit {

    private final Object target;

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
    }

    public Object getService(String serviceName) {
        if (serviceName == null) {
            throw new NullPointerException("serviceName can't be null");
        }

        if (serviceName.isEmpty()) {
            throw new IllegalArgumentException("serviceName can't be empty");
        }

        serviceName = serviceName.substring(0, 1).toLowerCase() + serviceName.substring(1);

        try {
            Field field = target.getClass().getField(serviceName);
            return field.get(target);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException(format("unknown field [%s]", serviceName), e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(format("inaccessable field [%s]", serviceName), e);
        }
    }

    public void onPartitionAdded(int partitionId) {
        try {
            Method method = target.getClass().getMethod("onPartitionAdded", Integer.TYPE);
            method.invoke(target, partitionId);
        } catch (NoSuchMethodException e) {
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public void onPartitionRemoved(int partitionId) {
        try {
            Method method = target.getClass().getMethod("onPartitionRemoved", Integer.TYPE);
            method.invoke(target, partitionId);
        } catch (NoSuchMethodException e) {
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public void onStart() {
        try {
            Method method = target.getClass().getMethod("onStart");
            method.invoke(target);
        } catch (NoSuchMethodException e) {
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public void onStop() {
        try {
            Method method = target.getClass().getMethod("onStop");
            method.invoke(target);
        } catch (NoSuchMethodException e) {
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
