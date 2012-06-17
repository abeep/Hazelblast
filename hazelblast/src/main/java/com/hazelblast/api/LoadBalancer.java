package com.hazelblast.api;

import com.hazelcast.core.Member;

/**
 * A LoadBalancer is responsible for selecting a {@link Member} from the members of a {@link com.hazelcast.core.Cluster}.
 *
 * Each implementation should have at least a public constructor has a single argument of type
 * {@link com.hazelcast.core.HazelcastInstance}.
 */
public interface LoadBalancer {

    /**
     * Gets the next member that can be used to send a message to.
     *
     * @return the next member.
     * @throws com.hazelblast.api.exceptions.NoMemberAvailableException if no member can be found.
     */
    Member getNext();
}
