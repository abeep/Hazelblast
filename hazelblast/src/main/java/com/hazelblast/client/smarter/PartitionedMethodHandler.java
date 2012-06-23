package com.hazelblast.client.smarter;

import com.hazelblast.client.annotations.PartitionKey;
import com.hazelblast.client.annotations.Partitioned;
import com.hazelblast.client.exceptions.RemoteMethodTimeoutException;
import com.hazelblast.client.router.Router;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.PartitionAware;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import static java.lang.String.format;

public class PartitionedMethodHandler extends ProxyMethodHandler {

    @Override
    public Class<? extends Annotation> getAnnotation() {
        return Partitioned.class;
    }

    public Object invoke(Object proxy, ProxyMethod proxyMethod, Object[] args) throws Throwable {
        Object partitionKey = getPartitionKey(proxyMethod, args);

        Callable callable = proxyProvider.remoteMethodInvocationFactory.create(
                proxyProvider.sliceName, proxyMethod.method.getDeclaringClass().getSimpleName(), proxyMethod.method.getName(),
                args, proxyMethod.argTypes, partitionKey);

        Future future = proxyProvider.executorService.submit(callable);
        try {
            return future.get(proxyMethod.timeoutMs, TimeUnit.MILLISECONDS);
        } catch (MemberLeftException e) {
            throw e;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause == null) {
                cause = new MemberLeftException();
            } else {
                StackTraceElement[] clientSideStackTrace = Thread.currentThread().getStackTrace();
                fixStackTrace(cause, clientSideStackTrace);
            }
            throw cause;
        } catch (TimeoutException e) {
            if (proxyMethod.interruptOnTimeout) {
                future.cancel(true);
            }

            throw new RemoteMethodTimeoutException(
                    format("method '%s' failed to complete in the %s ms",
                            proxyMethod.method.toString(), proxyMethod.timeoutMs), e);
        }
    }

    private Object getPartitionKey(ProxyMethod methodInfo, Object[] args) {
        Object arg = args[methodInfo.partitionKeyIndex];
        if (arg == null) {
            throw new NullPointerException(format("The @PartitionKey argument of partitioned method '%s' can't be null",
                    methodInfo.method));
        }

        Object partitionKey;
        if (methodInfo.partitionKeyProperty == null) {
            if (arg instanceof PartitionAware) {
                partitionKey = ((PartitionAware) arg).getPartitionKey();
                if (partitionKey == null) {
                    throw new NullPointerException(
                            format("The @PartitionKey argument of PartitionAware type '%s' of partitioned method '%s' can't return null for PartitionAware.getPartitionKey()",
                                    args[methodInfo.partitionKeyIndex].getClass().getName(), methodInfo.method));
                }
            } else {
                partitionKey = arg;
            }
        } else {
            try {
                partitionKey = methodInfo.partitionKeyProperty.invoke(arg);

                if (partitionKey == null) {
                    throw new NullPointerException(
                            format("The @PartitionKey argument of PartitionAware type '%s' of partitioned method '%s' can't return null for '%s'",
                                    args[methodInfo.partitionKeyIndex].getClass().getName(), methodInfo.method, methodInfo.partitionKeyProperty));

                }

            } catch (InvocationTargetException e) {
                throw new IllegalArgumentException(format("[%s] The @PartitionKey.property method '%s' failed to be invoked",
                        methodInfo.method, methodInfo.partitionKeyProperty), e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(format("[%s] The @PartitionKey.property method '%s' failed to be invoked",
                        methodInfo.method, methodInfo.partitionKeyProperty), e);
            }
        }

        return partitionKey;
    }

    public ProxyMethod analyze(Method method) {
        Annotation annotation = method.getAnnotation(Partitioned.class);

        Method partitionKeyProperty = null;
        Router loadBalancer = null;

        Partitioned partitionedAnnotation = (Partitioned) annotation;
        boolean interruptOnTimeout = partitionedAnnotation.interruptOnTimeout();
        long timeoutMs = partitionedAnnotation.timeoutMs();
        PartitionKeyInfo partitionKeyInfo = getPartitionKeyInfo(method);
        int partitionKeyIndex = partitionKeyInfo.index;

        if (partitionKeyInfo.property != null) {
            Class argType = method.getParameterTypes()[partitionKeyIndex];

            try {
                partitionKeyProperty = argType.getMethod(partitionKeyInfo.property);
                partitionKeyProperty.setAccessible(true);
                if (partitionKeyProperty.getReturnType().equals(Void.class)) {
                    throw new IllegalArgumentException(
                            format("Argument with index '%s' of type '%s' in PartitionedMethod '%s' has an invalid @PartitionKey.property configuration. " +
                                    "The property method '%s' can't return void", partitionKeyIndex + 1, argType.getName(), method, partitionKeyProperty));
                }
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException(
                        format("Argument with index '%s' of type '%s' in PartitionedMethod '%s' has an invalid @PartitionKey.property configuration. " +
                                "The property method '%s' doesn't exist", partitionKeyIndex + 1, argType.getName(), method, partitionKeyProperty));
            }
        }

        return new ProxyMethod(method, partitionKeyIndex, partitionKeyProperty, timeoutMs, interruptOnTimeout, loadBalancer, this);
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
