package com.hazelblast.client.impl;

import com.hazelblast.client.annotations.LoadBalanced;
import com.hazelblast.client.router.NoOpRouter;
import com.hazelblast.client.router.Router;
import com.hazelcast.core.HazelcastInstance;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static java.lang.String.format;


/**
 * A {@link MethodInvocationHandlerFactory} responsible for dealing with load balanced methods.
 *
 * @author  Peter Veentjer.
 */
public class LoadBalancedMethodInvocationHandlerFactory extends RoutedMethodInvocationHandlerFactory {

    @Override
    public Class<? extends Annotation> getAnnotationClass() {
        return LoadBalanced.class;
    }

    public MethodInvocationHandler build(Method method) {
        LoadBalanced annotation = method.getAnnotation(LoadBalanced.class);

        long timeoutMs;
        boolean interruptOnTimeout;
        Router loadBalancer = null;
        timeoutMs = annotation.timeoutMs();
        interruptOnTimeout = annotation.interruptOnTimeout();

        Class<? extends Router> routerClass = annotation.loadBalancer();
        if (!routerClass.equals(NoOpRouter.class)) {
            try {
                Constructor<? extends Router> constructor = routerClass.getConstructor(HazelcastInstance.class);
                loadBalancer = constructor.newInstance(hazelcastInstance);
            } catch (InstantiationException e) {
                throw new IllegalArgumentException(format("Failed to instantiate Router class '%s'", routerClass.getName()), e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(format("Failed to instantiate Router class '%s'", routerClass.getName()), e);
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException(format("Failed to instantiate Router class '%s'", routerClass.getName()), e);
            } catch (InvocationTargetException e) {
                throw new IllegalArgumentException(format("Failed to instantiate Router class '%s'", routerClass.getName()), e);
            }
        }

        return new RoutedMethodInvocationHandler(method, timeoutMs, interruptOnTimeout, loadBalancer);
    }
}
