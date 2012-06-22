package com.hazelblast.client.loadbalancers;

import com.hazelcast.core.Member;

import java.lang.reflect.Method;

/**
 * A ContentBasedLoadBalancer is responsible for selecting a {@link Member} from the members of a
 * {@link com.hazelcast.core.Cluster}. The difference between this loadbalancer and the
 * {@link com.hazelcast.impl.ExecutionLoadBalancer} is that the former can do the routing based on the content that is
 * going to be send and the latter not. So a loadbalancer could be written that inspects the arguments and decides to
 * which nodes the request should be send.
 *
 * Each implementation should have at least one public constructor that has a single argument of type
 * {@link com.hazelcast.core.HazelcastInstance}.
 *
 * @author Peter Veentjer.
 */
public interface ContentBasedLoadBalancer {

    /**
     * Gets the next member that can be used to send a message to.
     *
     * @param method the method that is called
     * @param  args the arguments of the method being called.
     * @return the next member.
     * @throws com.hazelblast.server.exceptions.NoMemberAvailableException if no member can be found.
     */
    Member getNext(Method method, Object[] args);
}
