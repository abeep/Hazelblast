package com.hazelblast.client;

import com.hazelblast.api.*;
import com.hazelblast.server.PuServer;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiTask;
import com.hazelcast.core.PartitionAware;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

import static java.lang.String.format;

public final class ProxyProvider {

    private final ExecutorService executorService = Hazelcast.getExecutorService("calls");
    private final ConcurrentMap<Class, Object> proxies = new ConcurrentHashMap<Class, Object>();
    private String puName;

    public ProxyProvider(){
        this("default");
    }

    public ProxyProvider(String puName){
        if(puName == null){
            throw new NullPointerException("puName can't be null");
        }
        this.puName = puName;
    }

    public <T> T getProxy(Class<T> clazz) {
        if (clazz == null) {
            throw new NullPointerException("clazz can't be null");
        }

        Object proxy = proxies.get(clazz);
        if (proxy == null) {
            if (!clazz.isInterface()) {
                throw new IllegalAccessError(format("clazz [%s] is not an interface", clazz));
            }

            proxy = Proxy.newProxyInstance(
                    clazz.getClassLoader(),
                    new Class[]{clazz},
                    new InvocationHandlerImpl(clazz));
            Object oldProxy = proxies.putIfAbsent(clazz, proxy);
            proxy = oldProxy == null ? proxy : oldProxy;
        }

        return (T) proxy;
    }

    private enum MethodType {
        FORK_JOIN,
        PARTITIONED,
        LOAD_BALANCED
    }

    private class InvocationHandlerImpl implements InvocationHandler {
        private final Class clazz;

        private InvocationHandlerImpl(Class clazz) {
            this.clazz = clazz;
        }

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (getMethodType(method)) {
                case FORK_JOIN:
                    return invokeForkJoin(method, args);
                case PARTITIONED:
                    return invokePartitioned(method, args);
                case LOAD_BALANCED:
                    return invokeLoadBalancer(method, args);
                default:
                    throw new RuntimeException();
            }
        }

        private Object invokeLoadBalancer(Method method, Object[] args) throws ExecutionException, InterruptedException {
            LoadBalancedMethodInvocation task = new LoadBalancedMethodInvocation(puName,clazz.getSimpleName(), method.getName(), args);

            Future future = executorService.submit(task);
            return future.get();
        }

        private Object invokePartitioned(Method method, Object[] args) throws ExecutionException, InterruptedException {
            int routingIDIndex = getPartitionKey(method);
            if (routingIDIndex == -1) {
                throw new IllegalArgumentException("No routingId is found arguments of on method: " + method);
            }

            PartitionedMethodInvocation task = new PartitionedMethodInvocation(puName,clazz.getSimpleName(), method.getName(), args, routingIDIndex);

            Future future = executorService.submit(task);
            return future.get();
        }

        private Object invokeForkJoin(Method method, Object[] args) throws ExecutionException, InterruptedException {
            LoadBalancedMethodInvocation callable = new LoadBalancedMethodInvocation(puName,clazz.getSimpleName(), method.getName(), args);
            MultiTask task = new MultiTask(callable, getPuMembers());
            executorService.execute(task);
            task.get();
            return null;
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

        private MethodType getMethodType(Method method) {
            if (method.getAnnotation(ForkJoin.class) != null) {
                return MethodType.FORK_JOIN;
            } else if (method.getAnnotation(LoadBalanced.class) != null) {
                return MethodType.LOAD_BALANCED;
            } else if (method.getAnnotation(Partitioned.class) != null) {
                return MethodType.PARTITIONED;
            } else {
                throw new IllegalArgumentException("Could not determine remoting type of method: " + method);
            }
        }

        //all this looking up could be done up front.
        private int getPartitionKey(Method method) {
            Annotation[][] annotations = method.getParameterAnnotations();

            for (int argIndex = 0; argIndex < annotations.length; argIndex++) {
                Annotation[] argumentAnnotations = annotations[argIndex];
                for (int annotationIndex = 0; annotationIndex < argumentAnnotations.length; annotationIndex++) {
                    Annotation annotation = argumentAnnotations[annotationIndex];
                    if (annotation instanceof RoutingId) {
                        return argIndex;
                    }
                }
            }

            return -1;
        }
    }

    static class LoadBalancedMethodInvocation implements Callable, Serializable {
        private final String serviceName;
        private final String methodName;
        private final Object[] args;
        private final String puName;

        LoadBalancedMethodInvocation(String puName, String serviceName, String methodName, Object[] args) {
            this.puName = puName;
            this.serviceName = serviceName;
            this.methodName = methodName;
            this.args = args == null ? new Object[]{} : args;
        }

        public Object call() throws Exception {
            ProcessingUnit pu = PuServer.getProcessingUnit(puName);

            Object service = pu.getService(serviceName);

            Class[] argTypes = new Class[args.length];
            for (int k = 0; k < argTypes.length; k++) {
                argTypes[k] = args[k].getClass();
            }

            Method method = service.getClass().getMethod(methodName, argTypes);
            return method.invoke(service, args);
        }
    }

    static class PartitionedMethodInvocation implements Callable, PartitionAware, Serializable {
        private final String puName;
        private final String serviceName;
        private final String methodName;
        private final Object[] args;
        private final int routingIdIndex;

        PartitionedMethodInvocation(String puName, String serviceName, String methodName, Object[] args, int routingIDIndex) {
            this.puName = puName;
            this.serviceName = serviceName;
            this.methodName = methodName;
            this.args = args;
            this.routingIdIndex = routingIDIndex;
        }

        public Object call() throws Exception {
            ProcessingUnit pu = PuServer.getProcessingUnit(puName);

            Object service = pu.getService(serviceName);

            Class[] argTypes = new Class[args.length];
            for (int k = 0; k < argTypes.length; k++) {
                argTypes[k] = args[k].getClass();
            }

            Method method = service.getClass().getMethod(methodName, argTypes);
            return method.invoke(service, args);
        }

        public Object getPartitionKey() {
            return args[routingIdIndex];
        }
    }
}
