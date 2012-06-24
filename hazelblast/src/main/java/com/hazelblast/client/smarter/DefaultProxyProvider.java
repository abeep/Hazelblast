package com.hazelblast.client.smarter;


import com.hazelblast.client.ProxyProvider;
import com.hazelblast.client.RemoteMethodInvocationFactory;
import com.hazelblast.client.SerializableRemoteMethodInvocationFactory;
import com.hazelblast.client.annotations.DistributedService;
import com.hazelblast.server.Slice;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import static com.hazelblast.utils.Arguments.notNull;
import static java.lang.String.format;

public final class DefaultProxyProvider implements ProxyProvider {

    protected final ILogger logger;
    protected final HazelcastInstance hazelcastInstance;
    protected final ExecutorService executorService;
    protected final Cluster cluster;
    protected final String sliceName;
    protected volatile RemoteMethodInvocationFactory remoteMethodInvocationFactory = SerializableRemoteMethodInvocationFactory.INSTANCE;

    private final LocalMethodInvocationHandlerFactory localMethodInvocationHandlerFactory = new LocalMethodInvocationHandlerFactory();
    private final ConcurrentMap<Class, Object> proxies = new ConcurrentHashMap<Class, Object>();
    private final ConcurrentMap<Class<? extends Annotation>, MethodInvocationHandlerFactory> methodInvocationHandlerFactories
            = new ConcurrentHashMap<Class<? extends Annotation>, MethodInvocationHandlerFactory>();

    /**
     * Creates a new ProxyProvider that connects to the 'default' Slice.
     */
    public DefaultProxyProvider() {
        this(Slice.DEFAULT_NAME, Hazelcast.getDefaultInstance());
    }

    public DefaultProxyProvider(HazelcastInstance hazelcastInstance) {
        this(Slice.DEFAULT_NAME, hazelcastInstance);
    }

    /**
     * Creates a ProxyProvider that connects to a Slice with the given name
     *
     * @param sliceName         the Slice to connect to.
     * @param hazelcastInstance the HazelcastInstance
     * @throws NullPointerException if sliceName or executorService is null.
     */
    public DefaultProxyProvider(String sliceName, HazelcastInstance hazelcastInstance) {
        this(notNull("sliceName", sliceName),
                notNull("hazelcastInstance", hazelcastInstance),
                hazelcastInstance.getExecutorService());
    }

    /**
     * Creates a ProxyProvider that connects to a Slice with the given name
     *
     * @param sliceName         the Slice to connect to.
     * @param hazelcastInstance the HazelcastInstance
     * @param executorService   the executor service used. Make sure it belongs to the hazelcastInstance.
     * @throws NullPointerException if sliceName, hazelcastInstance or executorService is null.
     */
    public DefaultProxyProvider(String sliceName, HazelcastInstance hazelcastInstance, ExecutorService executorService) {
        this.sliceName = notNull("sliceName", sliceName);
        this.hazelcastInstance = notNull("hazelcastInstance", hazelcastInstance);
        this.executorService = notNull("executorService", executorService);
        this.cluster = hazelcastInstance.getCluster();
        this.logger = hazelcastInstance.getLoggingService().getLogger(DefaultProxyProvider.class.getName());
        registerMethodInvocationHandlerFactory(new LoadBalancedMethodInvocationHandlerFactory());
        registerMethodInvocationHandlerFactory(new PartitionedMethodInvocationHandlerFactory());

        for (MethodInvocationHandlerFactory invocationHandlerFactory : methodInvocationHandlerFactories.values()) {
            invocationHandlerFactory.proxyProvider = this;
        }
    }

    /**
     * Adds a MethodInvocationHandlerFactory to this DefaultProxyProvider. By adding additional MethodInvocationHandlerFactory,
     * one can customize the behavior of this ProxyProvider.
     * <p/>
     * If there already exists a factory that deals with the annotation class, it will be overwritten.
     *
     * @param factory the ProxyMethodHandler to add.
     * @return the previous ProxyMethodHandler, or null of non exists.
     * @throws NullPointerException     if handler is null.
     * @throws IllegalArgumentException if the handler already is registered to a different ProxyProvider.
     */
    public MethodInvocationHandlerFactory registerMethodInvocationHandlerFactory(MethodInvocationHandlerFactory factory) {
        notNull("factory", factory);

        if (factory.proxyProvider != null) {
            throw new IllegalArgumentException("factory already added to a ProxyProvider");
        }
        factory.setProxyProvider(this);
        return methodInvocationHandlerFactories.put(factory.getAnnotationClass(), factory);
    }

    /**
     * Removes the MethodInvocationHandlerFactory for the given annotation class.
     *
     * @param annotationClass the Class of the Annotation
     * @return the removed MethodInvocationHandlerFactory, or null of on exists.
     */
    public MethodInvocationHandlerFactory unregisterMethodInvocationHandlerFactory(Class<Annotation> annotationClass) {
        notNull("annotationClass", annotationClass);
        return methodInvocationHandlerFactories.remove(annotationClass);
    }

    /**
     * Gets the MethodInvocationHandlerFactory for the given annotation class.
     *
     * @param annotationClass the class of the annotation to get the MethodInvocationHandlerFactory for.
     * @return the found MethodInvocationHandlerFactory or null if none exists.
     * @throws NullPointerException if annotation class is null.
     */
    public MethodInvocationHandlerFactory getMethodInvocationHandlerFactory(Class<Annotation> annotationClass) {
        notNull("annotationClass", annotationClass);
        return methodInvocationHandlerFactories.get(annotationClass);
    }

    /**
     * Gets a (copy) of all Annotation classes that are registered in this ProxyProvider.
     *
     * @return a set containing all Annotation classes that are registered.
     */
    public Set<Class<? extends Annotation>> getRegisteredAnnotations() {
        return new HashSet<Class<? extends Annotation>>(methodInvocationHandlerFactories.keySet());
    }

    /**
     * Returns the name of the Slice this ProxyProvider is going to call.
     *
     * @return the name of the Slice.
     */
    public String getSliceName() {
        return sliceName;
    }

    /**
     * Gets the {@link HazelcastInstance} this DefaultProxyProvider uses to execute distributed calls.
     *
     * @return the HazelcastInstance. The returned value will never be null.
     */
    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
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


    public <T> T getProxy(Class<T> distributedServiceClass) {
        notNull("distributedServiceClass", distributedServiceClass);

        Object proxy = proxies.get(distributedServiceClass);
        if (proxy == null) {
            DistributedServiceInvocationHandler invocationHandler = buildDistributedServiceInvocationHandler(distributedServiceClass);

            proxy = Proxy.newProxyInstance(
                    distributedServiceClass.getClassLoader(),
                    new Class[]{distributedServiceClass},
                    invocationHandler);
            Object oldProxy = proxies.putIfAbsent(distributedServiceClass, proxy);
            proxy = oldProxy == null ? proxy : oldProxy;
        }

        return (T) proxy;
    }

    private DistributedServiceInvocationHandler buildDistributedServiceInvocationHandler(Class distributedServiceClass) {
        if (!distributedServiceClass.isInterface()) {
            throw new IllegalArgumentException(format("Class [%s] is not an interface so is not allowed to be proxied",
                    distributedServiceClass));
        }

        Map<Method, MethodInvocationHandler> methodHandlers = new HashMap<Method, MethodInvocationHandler>();

        for (Class interfaze : getAllInterfaces(distributedServiceClass)) {
            if (!interfaze.isAnnotationPresent(DistributedService.class)) {
                throw new IllegalArgumentException(
                        format("Interface [%s] is not implementing the [%s] annotation so is not allowed to be proxied",
                                interfaze, DistributedService.class.getName()));
            }

            for (Method method : interfaze.getMethods()) {
                Annotation annotation = getRegisteredAnnotations(method);
                MethodInvocationHandlerFactory invocationHandlerFactory = annotation == null ? new LocalMethodInvocationHandlerFactory() : methodInvocationHandlerFactories.get(annotation.annotationType());
                MethodInvocationHandler methodInvocationHandler = invocationHandlerFactory.build(method);
                methodHandlers.put(method, methodInvocationHandler);
            }
        }

        try {
            Method equalsMethod = Object.class.getMethod("equals", Object.class);
            methodHandlers.put(equalsMethod, localMethodInvocationHandlerFactory.build(equalsMethod));

            Method toStringMethod = Object.class.getMethod("toString");
            methodHandlers.put(toStringMethod, localMethodInvocationHandlerFactory.build(toStringMethod));

            Method hashCodeMethod = Object.class.getMethod("hashCode");
            methodHandlers.put(hashCodeMethod, localMethodInvocationHandlerFactory.build(hashCodeMethod));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("should not happen!", e);
        }

        return new DistributedServiceInvocationHandler(methodHandlers);
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

    private Annotation getRegisteredAnnotations(Method method) {
        List<Annotation> annotations = new LinkedList<Annotation>();

        for (Class<? extends Annotation> annotationClass : methodInvocationHandlerFactories.keySet()) {
            Annotation annotation = method.getAnnotation(annotationClass);
            if (annotation != null) {
                annotations.add(annotation);
            }
        }

        if (annotations.isEmpty()) {
            throw new IllegalArgumentException(
                    format("Method '%s' has no annotations needed to make a distributed call", method));
        }

        if (annotations.size() > 1) {
            throw new IllegalArgumentException(
                    format("Ambiguous annotations. Method '%s' has too many remoting to make a distributed call, found: %s", method, annotations));
        }

        return annotations.get(0);
    }

    private class DistributedServiceInvocationHandler implements InvocationHandler {

        private final Map<Method, MethodInvocationHandler> proxiedMethods;

        public DistributedServiceInvocationHandler(Map<Method, MethodInvocationHandler> proxiedMethods) {
            this.proxiedMethods = proxiedMethods;
        }

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            MethodInvocationHandler methodInvocationHandler = proxiedMethods.get(method);
            return methodInvocationHandler.invoke(proxy, args);
        }
    }
}
