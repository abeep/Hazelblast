package com.hazelblast.client;

import com.hazelblast.api.*;
import com.hazelblast.server.ServiceContextServer;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiTask;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;

import static com.hazelblast.utils.Arguments.notNull;
import static java.lang.String.format;

/**
 * The default {@link ProxyProvider} implementation.
 * <p/>
 * By default the DefaultProxyProvider is configured with the {@link SerializableRemoteMethodInvocationFactory}, so
 * it relies on the standard java serialization mechanism. If you want to use a different serialization mechanism, a
 * different factory can be injected.
 *
 * @author Peter Veentjer.
 */
public final class DefaultProxyProvider implements ProxyProvider {

    private final static ILogger logger = Logger.getLogger(DefaultProxyProvider.class.getName());

    private final ExecutorService executorService;
    private final ConcurrentMap<Class, Object> proxies = new ConcurrentHashMap<Class, Object>();
    private final String serviceContextName;
    private volatile RemoteMethodInvocationFactory remoteMethodInvocationFactory = SerializableRemoteMethodInvocationFactory.INSTANCE;

    /**
     * Creates a new ProxyProvider that connects to the 'default' ServiceContext.
     */
    public DefaultProxyProvider() {
        this(ServiceContextServer.DEFAULT_PU_NAME, Hazelcast.getExecutorService());
    }

    /**
     * Creates a ProxyProvider that connects to a ServiceContext with the given name
     *
     * @param serviceContextName the ServiceContext to connect to.
     * @param executorService    the (Hazelcast) ExecutorService used to execute the remote calls on.
     * @throws NullPointerException if serviceContextName or executorService is null.
     */
    public DefaultProxyProvider(String serviceContextName, ExecutorService executorService) {
        this.serviceContextName = notNull("serviceContextName", serviceContextName);
        this.executorService = notNull("executorService", executorService);
    }


    /**
     * Returns the name of the ServiceContext this ProxyProvider is going to call.
     *
     * @return the name of the ServiceContext.
     */
    public String getServiceContextName() {
        return serviceContextName;
    }

    /**
     * Gets the RemoteMethodInvocationFactory.
     *
     * @return the RemoteMethodInvocationFactory.
     */
    public RemoteMethodInvocationFactory getRemoteMethodInvocationFactory() {
        return remoteMethodInvocationFactory;
    }

    /**
     * Sets the RemoteMethodInvocationFactory.
     * <p/>
     * A volatile field is used to store the RemoteMethodInvocationFactory.
     *
     * @param remoteMethodInvocationFactory the new RemoteMethodInvocationFactory.
     * @throws NullPointerException if remoteMethodInvocationFactory is null.
     */
    public void setRemoteMethodInvocationFactory(RemoteMethodInvocationFactory remoteMethodInvocationFactory) {
        this.remoteMethodInvocationFactory = notNull("remoteMethodInvocationFactory", remoteMethodInvocationFactory);
    }

    public <T> T getProxy(Class<T> targetInterface) {
        notNull("targetInterface", targetInterface);

        //TODO: Improved logging

        Object proxy = proxies.get(targetInterface);
        if (proxy == null) {
            RemoteInterfaceInfo remoteInterfaceInfo = analyzeInterface(targetInterface);


            proxy = Proxy.newProxyInstance(
                    targetInterface.getClassLoader(),
                    new Class[]{targetInterface},
                    new InvocationHandlerImpl(remoteInterfaceInfo));
            Object oldProxy = proxies.putIfAbsent(targetInterface, proxy);
            proxy = oldProxy == null ? proxy : oldProxy;
        }

        return (T) proxy;
    }

    private RemoteInterfaceInfo analyzeInterface(Class targetInterface) {
        if (!targetInterface.isInterface()) {
            throw new IllegalArgumentException(format("Class [%s] is not an interface so is not allowed to be proxied",
                    targetInterface));
        }

        Map<Method, RemoteMethodInfo> methodInfoMap = new HashMap<Method, RemoteMethodInfo>();

        for (Class interfaze : getAllInterfaces(targetInterface)) {
            if (!interfaze.isAnnotationPresent(RemoteInterface.class)) {
                throw new IllegalArgumentException(
                        format("targetInterface [%s] is not implementing the [%s] annotation so is not allowed to be proxied",
                                interfaze, RemoteInterface.class.getName()));
            }

            for (Method method : interfaze.getMethods()) {
                RemoteMethodInfo methodInfo = analyzeMethod(method);
                methodInfoMap.put(method, methodInfo);
            }
        }

        return new RemoteInterfaceInfo(targetInterface, methodInfoMap);
    }

    private static Set<Class> getAllInterfaces(Class targetInterface) {
        Set<Class> result = new HashSet<Class>();
        Queue<Class> todo = new LinkedList<Class>();
        todo.add(targetInterface);

        for (; ; ) {
            Class x = todo.poll();
            if (x == null) {
                break;
            }

            result.add(x);
            for (Class superInterface : x.getInterfaces()) {
                if (!result.contains(superInterface)) {
                    todo.add(superInterface);
                }
            }
        }

        return result;
    }

    private static class RemoteInterfaceInfo {
        private final Class targetInterface;
        private final Map<Method, RemoteMethodInfo> methodInfoMap;

        public RemoteInterfaceInfo(Class targetInterface, Map<Method, RemoteMethodInfo> methodInfoMap) {
            this.targetInterface = targetInterface;
            this.methodInfoMap = methodInfoMap;
        }
    }

    private RemoteMethodInfo analyzeMethod(Method method) {
        List<MethodType> methodTypes = getMethodTypes(method);

        if (methodTypes.isEmpty()) {
            throw new IllegalArgumentException("Method '%s' is missing a remoting annotation");
        }

        if (methodTypes.size() > 1) {
            throw new IllegalArgumentException("Method '%s' has too many remoting annotations");
        }

        MethodType methodType = methodTypes.get(0);
        int partitionKeyIndex = -1;
        Method partitionKeyProperty = null;
        switch (methodType) {
            case FORK_JOIN:
                break;
            case LOAD_BALANCED:
                break;
            case PARTITIONED:
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

                PartitionKeyInfo partitionKeyInfo = partitionKeyInfoList.get(0);
                partitionKeyIndex = partitionKeyInfo.index;

                if (partitionKeyInfo.property != null) {
                    Class argType = method.getParameterTypes()[partitionKeyIndex];

                    try {
                        partitionKeyProperty = argType.getMethod(partitionKeyInfo.property);
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

                break;
            default:
                throw new IllegalStateException("Unrecognized method type: " + methodType);
        }

        return new RemoteMethodInfo(method, methodType, partitionKeyIndex, partitionKeyProperty);

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

    private static List<MethodType> getMethodTypes(Method method) {
        List<MethodType> types = new LinkedList<MethodType>();

        //if (method.getAnnotation(ForkJoin.class) != null) {
        //    types.add(MethodType.FORK_JOIN);
        //}

        if (method.getAnnotation(LoadBalanced.class) != null) {
            types.add(MethodType.LOAD_BALANCED);
        }

        if (method.getAnnotation(Partitioned.class) != null) {
            types.add(MethodType.PARTITIONED);
        }

        return types;
    }

    private class InvocationHandlerImpl implements InvocationHandler {

        private final RemoteInterfaceInfo remoteInterfaceInfo;

        InvocationHandlerImpl(RemoteInterfaceInfo remoteInterfaceInfo) {
            this.remoteInterfaceInfo = remoteInterfaceInfo;
        }

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            RemoteMethodInfo methodInfo = remoteInterfaceInfo.methodInfoMap.get(method);
            if (methodInfo == null) {
                return invokeNonProxied(proxy, method, args);
            }

            long startTimeNs = 0;
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, "Starting " + method);
                startTimeNs = System.nanoTime();
            }

            Object result;
            switch (methodInfo.methodType) {
                case FORK_JOIN:
                    result = invokeForkJoin(methodInfo, args);
                    break;
                case PARTITIONED:
                    result = invokePartitioned(methodInfo, args);
                    break;
                case LOAD_BALANCED:
                    result = invokeLoadBalancer(methodInfo, args);
                    break;
                default:
                    throw new RuntimeException("unhandeled method type: " + methodInfo.methodType);
            }

            if (logger.isLoggable(Level.FINE)) {
                long delayNs = System.nanoTime() - startTimeNs;
                logger.log(Level.FINE, "Completed " + method + " in " + (delayNs / 1000) + " microseconds");
            }

            return result;
        }

        private Object invokeNonProxied(Object proxy, Method method, Object[] args) {
            String methodName = method.getName();
            if (methodName.equals("toString")) {
                return remoteInterfaceInfo.targetInterface.getName() + "@" + System.identityHashCode(proxy);
            } else if (methodName.equals("hashCode")) {
                return System.identityHashCode(proxy);
            } else if (methodName.equals("equals")) {
                return proxy == args[0];
            } else {
                throw new RuntimeException(format("unhandled method '%s'", method));
            }
        }

        private Object invokeForkJoin(RemoteMethodInfo remoteMethodInfo, Object[] args) throws Throwable {

            Callable callable = remoteMethodInvocationFactory.create(
                    serviceContextName, remoteInterfaceInfo.targetInterface.getSimpleName(), remoteMethodInfo.method.getName(),
                    args, remoteMethodInfo.argTypes, null);

            MultiTask task = new MultiTask(callable, getPuMembers());
            executorService.execute(task);
            try {
                task.get();
            } catch (ExecutionException e) {
                throw e.getCause();
            }
            return null;
        }

        private Object invokeLoadBalancer(RemoteMethodInfo remoteMethodInfo, Object[] args) throws Throwable {
            Callable callable = remoteMethodInvocationFactory.create(
                    serviceContextName, remoteInterfaceInfo.targetInterface.getSimpleName(), remoteMethodInfo.method.getName(),
                    args, remoteMethodInfo.argTypes, null);

            Future future = executorService.submit(callable);
            try {
                return future.get();
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        }

        private Object invokePartitioned(RemoteMethodInfo remoteMethodInfo, Object[] args) throws Throwable {
            Object partitionKey = getPartitionKey(remoteMethodInfo, args);

            Callable callable = remoteMethodInvocationFactory.create(
                    serviceContextName, remoteInterfaceInfo.targetInterface.getSimpleName(), remoteMethodInfo.method.getName(),
                    args, remoteMethodInfo.argTypes, partitionKey);

            Future future = executorService.submit(callable);
            try {
                return future.get();
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        }

        private Object getPartitionKey(RemoteMethodInfo methodInfo, Object[] args) {
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
    }

    private Set<Member> getPuMembers() {
        Set<Member> result = new HashSet<Member>();

        for (Member member : Hazelcast.getCluster().getMembers()) {
            if (!member.isLiteMember()) {
                result.add(member);
            }
        }

        return result;
    }

    private static class RemoteMethodInfo {
        final Method method;
        final MethodType methodType;
        final int partitionKeyIndex;
        final Method partitionKeyProperty;
        final String[] argTypes;

        private RemoteMethodInfo(Method method, MethodType methodType, int partitionKeyIndex, Method partitionKeyProperty) {
            this.method = method;
            this.methodType = methodType;
            this.partitionKeyIndex = partitionKeyIndex;
            this.partitionKeyProperty = partitionKeyProperty;

            Class[] parameterTypes = method.getParameterTypes();
            argTypes = new String[parameterTypes.length];
            for (int k = 0; k < argTypes.length; k++) {
                argTypes[k] = parameterTypes[k].getName();
            }
        }
    }

    private enum MethodType {
        FORK_JOIN,
        PARTITIONED,
        LOAD_BALANCED
    }
}
