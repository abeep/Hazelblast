package com.hazelblast.client.smarter;

import com.hazelblast.client.annotations.LoadBalanced;
import com.hazelblast.client.exceptions.RemoteMethodTimeoutException;
import com.hazelblast.client.router.NoOpRouter;
import com.hazelblast.client.router.Router;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.MemberLeftException;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.*;

import static java.lang.String.format;

public class LoadBalancedMethodHandler extends ProxyMethodHandler{

    @Override
    public Class<? extends Annotation> getAnnotation() {
        return LoadBalanced.class;
    }

    public ProxyMethod analyze(Method method) {
        LoadBalanced annotation = method.getAnnotation(LoadBalanced.class);

        long timeoutMs;
        boolean interruptOnTimeout;
        int partitionKeyIndex = -1;
        Method partitionKeyProperty = null;
        Router loadBalancer = null;
        timeoutMs = annotation.timeoutMs();
        interruptOnTimeout = annotation.interruptOnTimeout();

        Class<? extends Router> loadBalancerClass = annotation.loadBalancer();
        if (!loadBalancerClass.equals(NoOpRouter.class)) {
            try {
                Constructor<? extends Router> constructor = loadBalancerClass.getConstructor(HazelcastInstance.class);
                loadBalancer = constructor.newInstance(proxyProvider.hazelcastInstance);
            } catch (InstantiationException e) {
                throw new IllegalArgumentException(format("Failed to instantiate Router class '%s'", loadBalancerClass.getName()), e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(format("Failed to instantiate Router class '%s'", loadBalancerClass.getName()), e);
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException(format("Failed to instantiate Router class '%s'", loadBalancerClass.getName()), e);
            } catch (InvocationTargetException e) {
                throw new IllegalArgumentException(format("Failed to instantiate Router class '%s'", loadBalancerClass.getName()), e);
            }
        }

        return new ProxyMethod(method, partitionKeyIndex, partitionKeyProperty, timeoutMs, interruptOnTimeout, loadBalancer, this);
    }

    public Object invoke(Object proxy, ProxyMethod proxyMethod, Object[] args)throws Throwable {
        Callable callable = proxyProvider.remoteMethodInvocationFactory.create(
                proxyProvider.sliceName, proxyMethod.method.getDeclaringClass().getSimpleName(), proxyMethod.method.getName(),
                args, proxyMethod.argTypes, null);

        Future future;
        if (proxyMethod.loadBalancer == null) {
            future = proxyProvider.executorService.submit(callable);
        } else {
            com.hazelcast.core.Member targetMember = proxyMethod.loadBalancer.getNext(proxyMethod.method, args);
            future = proxyProvider.executorService.submit(new DistributedTask(callable, targetMember));
        }

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


}
