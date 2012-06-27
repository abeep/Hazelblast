package com.hazelblast.client.basic;

/**
 * A handler for method invocations.
 *
 * When the proxy is generated in the {@link BasicProxyProvider} it returns a {@link java.lang.reflect.InvocationHandler}.
 * For every callable method on this interface, it will contain a {@link MethodInvocationHandler} that executes the
 * actual call.
 *
 * @author Peter Veentjer.
 */
public interface MethodInvocationHandler {

    Object invoke(Object proxy, Object[] args) throws Throwable;
}
