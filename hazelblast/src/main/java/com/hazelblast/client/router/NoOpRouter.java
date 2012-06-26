package com.hazelblast.client.router;

import java.lang.reflect.Method;

/**
 * A 'placeholder' {@link Router} that indicates to the ProxyProvider that it should rely
 * on loadbalancing higher up, e.g. if something is configured on the Executor.
 *
 * The problem with annotation properties is that they can't be null. So for the
 * {@link com.hazelblast.client.annotations.LoadBalanced#loadBalancer()} to 'not' be configured, this class is needed.
 *
 * @author Peter Veentjer.
 */
public final class NoOpRouter implements Router {

    public Target getTarget(Method method, Object[] args) {
        throw new UnsupportedOperationException();
    }
}
