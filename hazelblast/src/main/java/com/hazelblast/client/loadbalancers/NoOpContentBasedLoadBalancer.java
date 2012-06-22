package com.hazelblast.client.loadbalancers;

import com.hazelcast.core.Member;

import java.lang.reflect.Method;

/**
 * A 'placeholder' {@link ContentBasedLoadBalancer} that indicates to the ProxyProvider that it should rely
 * on loadbalancing higher up, e.g. if something is configured on the Executor.
 *
 * @author Peter Veentjer.
 */
public final class NoOpContentBasedLoadBalancer implements ContentBasedLoadBalancer {

    public Member getNext(Method method, Object[] args) {
        throw new UnsupportedOperationException();
    }
}
