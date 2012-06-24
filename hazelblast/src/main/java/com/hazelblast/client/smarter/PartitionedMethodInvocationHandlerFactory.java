package com.hazelblast.client.smarter;

import com.hazelblast.client.annotations.PartitionKey;
import com.hazelblast.client.annotations.Partitioned;
import com.hazelblast.client.router.PartitionRouter;
import com.hazelblast.client.router.Router;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;

import static java.lang.String.format;

public class PartitionedMethodInvocationHandlerFactory extends RoutedMethodInvocationHandlerFactory {

    @Override
    public Class<? extends Annotation> getAnnotationClass() {
        return Partitioned.class;
    }

    public MethodInvocationHandler build(Method partitionedMethod) {
        Annotation annotation = partitionedMethod.getAnnotation(Partitioned.class);

        Method propertyMethod = null;
        Field propertyField = null;
        Partitioned partitionedAnnotation = (Partitioned) annotation;
        boolean interruptOnTimeout = partitionedAnnotation.interruptOnTimeout();
        long timeoutMs = partitionedAnnotation.timeoutMs();
        PartitionKeyInfo partitionKeyInfo = getPartitionKeyInfo(partitionedMethod);
        int partitionKeyIndex = partitionKeyInfo.index;

        String propertyName = partitionKeyInfo.property;
        if (propertyName != null) {
            Class argType = partitionedMethod.getParameterTypes()[partitionKeyIndex];
            propertyMethod = getMethod(partitionedMethod, argType, propertyName);

            if (propertyMethod == null) {
                propertyMethod = getMethod(partitionedMethod, argType, "get" + uppercaseFirstLetter(propertyName));
            }

            if (propertyMethod == null) {
                propertyMethod = getMethod(partitionedMethod, argType, "is" + uppercaseFirstLetter(propertyName));
            }

            if (propertyMethod == null) {
                try {
                    propertyField = argType.getDeclaredField(propertyName);
                } catch (NoSuchFieldException e) {
                    throw new IllegalArgumentException(
                            format("Property '%s' of the the @PartitionKey argument of method '%s', doesn't point to an existing method or field", propertyMethod, partitionedMethod));

                }
            }
        }

        Router router = new PartitionRouter(hazelcastInstance, propertyMethod, propertyField, partitionKeyIndex);
        return new RoutedMethodInvocationHandler(partitionedMethod, timeoutMs, interruptOnTimeout, router);
    }

    private static String uppercaseFirstLetter(String s) {
        char[] stringArray = s.toCharArray();
        stringArray[0] = Character.toUpperCase(stringArray[0]);
        return new String(stringArray);
    }

    private Method getMethod(Method partitionedMethod, Class argType, String name) {
        try {
            Method propertyMethod = argType.getDeclaredMethod(name);
            propertyMethod.setAccessible(true);
            if (propertyMethod.getReturnType().equals(Void.class)) {
                throw new IllegalArgumentException(format("The property method '%s' of the the @PartitionKey argument of method '%s', can't return void", propertyMethod, partitionedMethod));
            }
            return propertyMethod;
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    private PartitionKeyInfo getPartitionKeyInfo(Method method) {
        if (method.getParameterTypes().length == 0) {
            throw new IllegalArgumentException(format("@Partitioned method '%s', should have a least 1 argument to use as @PartitionKey.", method));
        }

        List<PartitionKeyInfo> partitionKeyInfoList = getPartitionKeyIndex(method);
        if (partitionKeyInfoList.isEmpty()) {
            throw new IllegalArgumentException(format("@PartitionedMethod '%s' has no argument with the @PartitionKey annotation", method));
        }

        if (partitionKeyInfoList.size() > 1) {
            throw new IllegalArgumentException(format("@PartitionedMethod '%s' has too many arguments with the @PartitionKey annotation", method));
        }

        return partitionKeyInfoList.get(0);
    }

    static class PartitionKeyInfo {
        final int index;
        final String property;

        PartitionKeyInfo(int index, String property) {
            this.index = index;
            this.property = property;
        }
    }

    private static List<PartitionKeyInfo> getPartitionKeyIndex(Method method) {
        List<PartitionKeyInfo> result = new LinkedList<PartitionKeyInfo>();

        Annotation[][] annotations = method.getParameterAnnotations();

        for (int argIndex = 0; argIndex < annotations.length; argIndex++) {
            Annotation[] argumentAnnotations = annotations[argIndex];
            for (int annotationIndex = 0; annotationIndex < argumentAnnotations.length; annotationIndex++) {
                Annotation annotation = argumentAnnotations[annotationIndex];
                if (annotation instanceof PartitionKey) {
                    PartitionKey partitionKey = (PartitionKey) annotation;
                    String property = partitionKey.property();
                    result.add(new PartitionKeyInfo(argIndex, property.isEmpty() ? null : property));
                }
            }
        }

        return result;
    }
}
