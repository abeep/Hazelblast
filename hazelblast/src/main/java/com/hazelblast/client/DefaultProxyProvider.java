package com.hazelblast.client;

import com.hazelblast.api.*;
import com.hazelblast.api.exceptions.PartitionMovedException;
import com.hazelblast.api.exceptions.RemoteMethodTimeoutException;
import com.hazelblast.server.ServiceContextServer;
import com.hazelcast.core.*;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
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
 * <p/>
 * Proxy instances are cached; so requests for the same interface will return the same instance.
 *
 * @author Peter Veentjer.
 */
public final class DefaultProxyProvider implements ProxyProvider {

    private final ILogger logger;
    private final HazelcastInstance hazelcastInstance;
    private final ExecutorService executorService;
    private final Cluster cluster;
    private final ConcurrentMap<Class, Object> proxies = new ConcurrentHashMap<Class, Object>();
    private final String serviceContextName;
    private volatile RemoteMethodInvocationFactory remoteMethodInvocationFactory = SerializableRemoteMethodInvocationFactory.INSTANCE;

    /**
     * Creates a new ProxyProvider that connects to the 'default' ServiceContext.
     */
    public DefaultProxyProvider() {
        this(ServiceContextServer.DEFAULT_PU_NAME, Hazelcast.getDefaultInstance());
    }

    /**
     * Creates a ProxyProvider that connects to a ServiceContext with the given name
     *
     * @param serviceContextName the ServiceContext to connect to.
     * @param hazelcastInstance  the HazelcastInstance
     * @throws NullPointerException if serviceContextName or executorService is null.
     */
    public DefaultProxyProvider(String serviceContextName, HazelcastInstance hazelcastInstance) {
        this(notNull("serviceContextName", serviceContextName),
                notNull("hazelcastInstance", hazelcastInstance),
                hazelcastInstance.getExecutorService());
    }

    /**
     * Creates a ProxyProvider that connects to a ServiceContext with the given name
     *
     * @param serviceContextName the ServiceContext to connect to.
     * @param hazelcastInstance  the HazelcastInstance
     * @param executorService    the executor service used. Make sure it belongs to the hazelcastInstance.
     * @throws NullPointerException if serviceContextName, hazelcastInstance or executorService is null.
     */
    public DefaultProxyProvider(String serviceContextName, HazelcastInstance hazelcastInstance, ExecutorService executorService) {
        this.serviceContextName = notNull("serviceContextName", serviceContextName);
        this.hazelcastInstance = notNull("hazelcastInstance", hazelcastInstance);
        this.executorService = notNull("executorService", executorService);
        this.cluster = hazelcastInstance.getCluster();
        this.logger = hazelcastInstance.getLoggingService().getLogger(DefaultProxyProvider.class.getName());
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
        Annotation annotation = getRemotingAnnotation(method);

        long timeoutMs;
        int partitionKeyIndex = -1;
        Method partitionKeyProperty = null;
        MethodType methodType;
        LoadBalancer loadBalancer = null;
        if (annotation instanceof LoadBalanced) {
            LoadBalanced loadBalancedAnnotation = (LoadBalanced) annotation;
            timeoutMs = loadBalancedAnnotation.timeoutMs();
            methodType = MethodType.LOAD_BALANCED;

            Class<? extends LoadBalancer> loadBalancerClass = loadBalancedAnnotation.loadBalancer();
            try {
                Constructor<? extends LoadBalancer> constructor = loadBalancerClass.getConstructor(HazelcastInstance.class);
                loadBalancer = constructor.newInstance(hazelcastInstance);
            } catch (InstantiationException e) {
                throw new IllegalArgumentException(format("Failed to instantiate LoadBalancer class '%s'", loadBalancerClass.getName()), e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(format("Failed to instantiate LoadBalancer class '%s'", loadBalancerClass.getName()), e);
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException(format("Failed to instantiate LoadBalancer class '%s'", loadBalancerClass.getName()), e);
            } catch (InvocationTargetException e) {
                throw new IllegalArgumentException(format("Failed to instantiate LoadBalancer class '%s'", loadBalancerClass.getName()), e);
            }
        } else if (annotation instanceof Partitioned) {
            Partitioned partitionedAnnotation = (Partitioned) annotation;
            timeoutMs = partitionedAnnotation.timeoutMs();
            methodType = MethodType.PARTITIONED;
            PartitionKeyInfo partitionKeyInfo = getPartitionKeyInfo(method);
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

        } else {
            throw new IllegalStateException("Unrecognized method annotation: " + annotation);
        }

        return new RemoteMethodInfo(method, methodType, partitionKeyIndex, partitionKeyProperty, timeoutMs, loadBalancer);
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

    private Annotation getRemotingAnnotation(Method method) {
        List<Annotation> annotations = new LinkedList<Annotation>();

        Annotation loadBalanced = method.getAnnotation(LoadBalanced.class);
        if (loadBalanced != null) {
            annotations.add(loadBalanced);
        }

        Annotation partitioned = method.getAnnotation(Partitioned.class);
        if (partitioned != null) {
            annotations.add(partitioned);
        }

        if (annotations.isEmpty()) {
            throw new IllegalArgumentException("Method '%s' is missing a remoting annotation");
        }

        if (annotations.size() > 1) {
            throw new IllegalArgumentException("Method '%s' has too many remoting annotations");
        }

        return annotations.get(0);
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

            //TODO: Number of retries should be configurable in the future.
            while (true) {
                try {
                    Object result;
                    switch (methodInfo.methodType) {
                        case PARTITIONED:
                            result = invokePartitioned(methodInfo, args);
                            break;
                        case LOAD_BALANCED:
                            result = invokeLoadBalanced(methodInfo, args);
                            break;
                        default:
                            throw new RuntimeException("unhandled method type: " + methodInfo.methodType);
                    }

                    if (logger.isLoggable(Level.FINE)) {
                        long delayNs = System.nanoTime() - startTimeNs;
                        logger.log(Level.FINE, "Completed " + method + " in " + (delayNs / 1000) + " microseconds");
                    }

                    return result;
                } catch (PartitionMovedException e) {
                    logger.log(Level.INFO, "Method invocation was send to bad partition, retrying");
                    //we wait some, since it will take some time for this partition to be started on another node.
                    //there is no need to pound the system with useless requests.
                    Thread.sleep(100);
                } catch (MemberLeftException e) {
                    logger.log(Level.INFO, "Method invocation was send to a member that left, retrying");
                }
            }
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

            MultiTask task = new MultiTask(callable, getClusterMembers());
            executorService.execute(task);
            try {
                task.get(remoteMethodInfo.timeoutMs, TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                throw e.getCause();
            }
            return null;
        }

        private Object invokeLoadBalanced(RemoteMethodInfo remoteMethodInfo, Object[] args) throws Throwable {
            Callable callable = remoteMethodInvocationFactory.create(
                    serviceContextName, remoteInterfaceInfo.targetInterface.getSimpleName(), remoteMethodInfo.method.getName(),
                    args, remoteMethodInfo.argTypes, null);

            Member targetMember = remoteMethodInfo.loadBalancer.getNext();

            DistributedTask<String> task = new DistributedTask<String>(callable, targetMember);

            Future future = executorService.submit(task);
            try {
                return future.get(remoteMethodInfo.timeoutMs, TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause == null) {
                    cause = new MemberLeftException();
                }
                throw cause;
            } catch (TimeoutException e) {
                throw new RemoteMethodTimeoutException(
                        format("method '%s' failed to complete in the %s ms",
                                remoteMethodInfo.method.toString(), remoteMethodInfo.timeoutMs), e);
            }
        }

        private Object invokePartitioned(RemoteMethodInfo remoteMethodInfo, Object[] args) throws Throwable {
            Object partitionKey = getPartitionKey(remoteMethodInfo, args);

            Callable callable = remoteMethodInvocationFactory.create(
                    serviceContextName, remoteInterfaceInfo.targetInterface.getSimpleName(), remoteMethodInfo.method.getName(),
                    args, remoteMethodInfo.argTypes, partitionKey);

            Future future = executorService.submit(callable);
            try {
                return future.get(remoteMethodInfo.timeoutMs, TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                throw e.getCause();
            } catch (TimeoutException e) {
                throw new RemoteMethodTimeoutException(
                        format("method '%s' failed to complete in the %s ms",
                                remoteMethodInfo.method.toString(), remoteMethodInfo.timeoutMs), e);
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

    private Set<Member> getClusterMembers() {
        Set<Member> result = new HashSet<Member>();

        for (Member member : cluster.getMembers()) {
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
        final long timeoutMs;
        final LoadBalancer loadBalancer;

        private RemoteMethodInfo(Method method, MethodType methodType, int partitionKeyIndex,
                                 Method partitionKeyProperty, long timeoutMs, LoadBalancer loadBalancer) {
            this.method = method;
            this.methodType = methodType;
            this.partitionKeyIndex = partitionKeyIndex;
            this.partitionKeyProperty = partitionKeyProperty;
            this.timeoutMs = timeoutMs;
            this.loadBalancer = loadBalancer;

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
