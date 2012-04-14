package com.hazelblast.server.pojo;

import com.hazelblast.api.ProcessingUnit;
import com.hazelblast.api.PuFactory;
import com.hazelblast.api.ProcessingUnit;
import com.hazelblast.api.PuFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static java.lang.String.format;

public class PojoPuFactory implements PuFactory {

    public ProcessingUnit create(int partition) {
        return new PojoPu(partition);
    }

    static class PojoPu implements ProcessingUnit {

        private final int partition;
        private final Object target;

        public PojoPu(int partition) {
            this.partition = partition;

            String pojoClassName = System.getProperty("pojoPu");
            try {
                Class clazz = PojoPuFactory.class.getClassLoader().loadClass(pojoClassName);
                Constructor c = clazz.getConstructor(Integer.TYPE);
                target = c.newInstance(partition);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }

        public Object getService(String name) {
            if (name == null) {
                throw new NullPointerException("name can't be null");
            }

            if (name.isEmpty()) {
                throw new IllegalArgumentException("name can't be empty");
            }

            name = name.substring(0, 1).toLowerCase() + name.substring(1);

            try {
                Field field = target.getClass().getField(name);
                return field.get(target);
            } catch (NoSuchFieldException e) {
                throw new IllegalArgumentException(format("unknown field [%s]", name), e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(format("inaccessable field [%s]", name), e);
            }
        }

        public void start() {
            try {
                Method method = target.getClass().getMethod("start");
                method.invoke(target);
            } catch (NoSuchMethodException e) {
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        public void stop() {
            try {
                Method method = target.getClass().getMethod("stop");
                method.invoke(target);
            } catch (NoSuchMethodException e) {
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
