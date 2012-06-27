package com.hazelblast.client.router;

import java.lang.reflect.Method;

/**
 * A 'placeholder' {@link Router} that indicates to the ProxyProvider that it should rely
 * on loadbalancing higher up, e.g. if something is configured on the Executor.
 *
 * The problem with annotation properties is that they can't be null. So for the
 * {@link com.hazelblast.client.annotations.LoadBalanced#loadBalancer()} to 'not' be
 * configured, this class is needed.
 *
 * <h1>IMPORTANT!</h1>
 * Hazelcast allows requests to be send to lite members. In the future this is going
 * to be <a href="https://github.com/hazelcast/hazelcast/issues/63">configurable</a>. But till
 * then, using the NoOpRouter needs to be used with care because a loadbalancer needs to be used
 * that doesn't send request to lite members.
 *
 * @author Peter Veentjer.
 */
public final class NoOpRouter implements Router {

    public Target getTarget(Method method, Object[] args) {
        throw new UnsupportedOperationException();
    }
}
