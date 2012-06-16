package com.hazelblast.api;

import com.hazelcast.core.Member;

/**
 * Each implementation should have at least a public constructor has a single argument of type
 * {@link com.hazelcast.core.HazelcastInstance}.
 */
public interface LoadBalancer {

    Member findTargetMember();
}
