package com.hazelblast.client.basic;

import com.hazelblast.api.*;
import com.hazelblast.client.ProxyProvider;
import com.hazelblast.server.PartitionMovedException;
import com.hazelblast.server.ServiceContextServer;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;

import static com.hazelblast.utils.Arguments.notNull;
import static java.lang.String.format;

/**
 * The default {@link com.hazelblast.client.ProxyProvider} implementation.
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

    private final static ILogger logger = Logger.getLogger(DefaultProxyProvider.class.getName());


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
        private final Map<Method, MethodInvocationHandler> handlers = new HashMap<Method, MethodInvocationHandler>();

        InvocationHandlerImpl(RemoteInterfaceInfo remoteInterfaceInfo) {
            this.remoteInterfaceInfo = remoteInterfaceInfo;

            for (RemoteMethodInfo remoteMethodInfo : remoteInterfaceInfo.methodInfoMap.values()) {
                switch (remoteMethodInfo.methodType) {
                    case FORK_JOIN:
                        throw new RuntimeException();
                    case PARTITIONED:
                        handlers.put(remoteMethodInfo.method,
                                new PartitionedInvocationHandler(remoteInterfaceInfo, remoteMethodInfo, executorService, serviceContextName));
                        break;
                    case LOAD_BALANCED:
                        handlers.put(remoteMethodInfo.method,
                                new LoadBalancedMethodInvocationHandler(remoteInterfaceInfo, remoteMethodInfo, executorService, serviceContextName));
                        break;
                    default:
                        throw new IllegalArgumentException("unrecognized methodType: " + remoteMethodInfo.methodType);
                }
            }
        }

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            MethodInvocationHandler handler = handlers.get(method);
            if (handler.isLocal()) {
                return handler.invoke(proxy, args);
            }

            long startTimeNs = 0;
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, "Starting " + method);
                startTimeNs = System.nanoTime();
            }

            //TODO: Number of retries should be configurable in the future.
            while (true) {
                try {
                    Object result = handler.invoke(proxy, args);

                    if (logger.isLoggable(Level.FINE)) {
                        long delayNs = System.nanoTime() - startTimeNs;
                        logger.log(Level.FINE, "Completed " + method + " in " + (delayNs / 1000) + " microseconds");
                    }

                    return result;
                } catch (PartitionMovedException e) {
                    logger.log(Level.INFO, "Method invocation was send to bad partition, retrying");
                    Thread.sleep(100);
                }
            }
        }

    }
}
