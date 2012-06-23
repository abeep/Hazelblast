package com.hazelblast.client.smarter;


import com.hazelblast.client.ProxyProvider;
import com.hazelblast.client.RemoteMethodInvocationFactory;
import com.hazelblast.client.SerializableRemoteMethodInvocationFactory;
import com.hazelblast.client.annotations.DistributedService;
import com.hazelblast.server.Slice;
import com.hazelblast.server.exceptions.PartitionMovedException;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.logging.ILogger;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;

import static com.hazelblast.utils.Arguments.notNull;
import static java.lang.String.format;

public final class SmarterProxyProvider implements ProxyProvider {

    protected final ILogger logger;
    protected final HazelcastInstance hazelcastInstance;
    protected final ExecutorService executorService;
    protected final Cluster cluster;
    protected final String sliceName;
    protected volatile RemoteMethodInvocationFactory remoteMethodInvocationFactory = SerializableRemoteMethodInvocationFactory.INSTANCE;

    private final ConcurrentMap<Class, Object> proxies = new ConcurrentHashMap<Class, Object>();
    private final ConcurrentMap<Class<? extends Annotation>, ProxyMethodHandler> handlers = new ConcurrentHashMap<Class<? extends Annotation>, ProxyMethodHandler>();

    /**
     * Creates a new ProxyProvider that connects to the 'default' Slice.
     */
    public SmarterProxyProvider() {
        this(Slice.DEFAULT_NAME, Hazelcast.getDefaultInstance());
    }

    public SmarterProxyProvider(HazelcastInstance hazelcastInstance) {
        this(Slice.DEFAULT_NAME, hazelcastInstance);
    }

    /**
     * Creates a ProxyProvider that connects to a Slice with the given name
     *
     * @param sliceName         the Slice to connect to.
     * @param hazelcastInstance the HazelcastInstance
     * @throws NullPointerException if sliceName or executorService is null.
     */
    public SmarterProxyProvider(String sliceName, HazelcastInstance hazelcastInstance) {
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
    public SmarterProxyProvider(String sliceName, HazelcastInstance hazelcastInstance, ExecutorService executorService) {
        this.sliceName = notNull("sliceName", sliceName);
        this.hazelcastInstance = notNull("hazelcastInstance", hazelcastInstance);
        this.executorService = notNull("executorService", executorService);
        this.cluster = hazelcastInstance.getCluster();
        this.logger = hazelcastInstance.getLoggingService().getLogger(SmarterProxyProvider.class.getName());
        addProxyMethodHandler(new LoadBalancedMethodHandler());
        addProxyMethodHandler(new PartitionedMethodHandler());

        for (ProxyMethodHandler handler : handlers.values()) {
            handler.proxyProvider = this;
        }
    }

    /**
     * Returns the name of the Slice this ProxyProvider is going to call.
     *
     * @return the name of the Slice.
     */
    public String getSliceName() {
        return sliceName;
    }

    public ProxyMethodHandler addProxyMethodHandler(ProxyMethodHandler proxyMethodHandler) {
        notNull("proxyMethodHandler", proxyMethodHandler);
        if (proxyMethodHandler.proxyProvider != null) {
            throw new IllegalArgumentException("proxyMethodHandler already added to another ProxyProvider");
        }
        proxyMethodHandler.proxyProvider = this;
        return handlers.put(proxyMethodHandler.getAnnotation(), proxyMethodHandler);
    }

    public ProxyMethodHandler removeProxyMethodHandler(Class<Annotation> annotationClass){
        return handlers.remove(annotationClass);
    }

    public ProxyMethodHandler getProxyMethodHandler(Class<Annotation> annotationClass){
        return handlers.get(annotationClass);
    }

    public Set<Class<? extends Annotation>> getProxyHandlerAnnotations(){
        return new HashSet<Class<? extends Annotation>>(handlers.keySet());
    }

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

    public <T> T getProxy(Class<T> targetInterface) {
        notNull("targetInterface", targetInterface);

        Object proxy = proxies.get(targetInterface);
        if (proxy == null) {
            DistributedServiceInfo distributedServiceInfo = analyzeInterface(targetInterface);

            proxy = Proxy.newProxyInstance(
                    targetInterface.getClassLoader(),
                    new Class[]{targetInterface},
                    new InvocationHandlerImpl(distributedServiceInfo));
            Object oldProxy = proxies.putIfAbsent(targetInterface, proxy);
            proxy = oldProxy == null ? proxy : oldProxy;
        }

        return (T) proxy;
    }

    private DistributedServiceInfo analyzeInterface(Class targetInterface) {
        if (!targetInterface.isInterface()) {
            throw new IllegalArgumentException(format("Class [%s] is not an interface so is not allowed to be proxied",
                    targetInterface));
        }

        Map<Method, ProxyMethod> proxiedMethods = new HashMap<Method, ProxyMethod>();

        for (Class interfaze : getAllInterfaces(targetInterface)) {
            if (!interfaze.isAnnotationPresent(DistributedService.class)) {
                throw new IllegalArgumentException(
                        format("targetInterface [%s] is not implementing the [%s] annotation so is not allowed to be proxied",
                                interfaze, DistributedService.class.getName()));
            }

            for (Method method : interfaze.getMethods()) {
                Annotation annotation = getDistributedMethodAnnotation(method);
                ProxyMethodHandler handler = annotation == null ? new LocalMethodHandler() : handlers.get(annotation.annotationType());
                ProxyMethod proxyMethod = handler.analyze(method);
                proxiedMethods.put(method, proxyMethod);
            }


        }

        return new DistributedServiceInfo(targetInterface, proxiedMethods);
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

    private static class DistributedServiceInfo {
        private final Class targetInterface;
        private final Map<Method, ProxyMethod> proxiedMethods;

        public DistributedServiceInfo(Class targetInterface, Map<Method, ProxyMethod> proxiedMethods) {
            this.targetInterface = targetInterface;
            this.proxiedMethods = proxiedMethods;
        }
    }

    private Annotation getDistributedMethodAnnotation(Method method) {
        List<Annotation> annotations = new LinkedList<Annotation>();

        for (Class<? extends Annotation> annotationClass : handlers.keySet()) {
            Annotation annotation = method.getAnnotation(annotationClass);
            if (annotation != null) {
                annotations.add(annotation);
            }
        }

        if (annotations.isEmpty()) {
            throw new IllegalArgumentException("Method '%s' is missing a remoting annotation");
        }

        if (annotations.size() > 1) {
            throw new IllegalArgumentException("Method '%s' has too many remoting annotations");
        }

        return annotations.get(0);
    }

    private class InvocationHandlerImpl implements InvocationHandler {

        private final DistributedServiceInfo distributedServiceInfo;

        InvocationHandlerImpl(DistributedServiceInfo distributedServiceInfo) {
            this.distributedServiceInfo = distributedServiceInfo;
        }

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            ProxyMethod proxiedMethod = distributedServiceInfo.proxiedMethods.get(method);
            if (proxiedMethod == null) {
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
                    Object result = proxiedMethod.invoke(proxy, args);

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
                return distributedServiceInfo.targetInterface.getName() + "@" + System.identityHashCode(proxy);
            } else if (methodName.equals("hashCode")) {
                return System.identityHashCode(proxy);
            } else if (methodName.equals("equals")) {
                return proxy == args[0];
            } else {
                throw new RuntimeException(format("unhandled method '%s'", method));
            }
        }
    }
}
