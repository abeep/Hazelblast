package com.hazelblast.server.pojoslice;

import com.hazelcast.core.HazelcastInstance;

/**
 * If a Pojo is responsible for creating the {@link HazelcastInstance}, this interface needs to be implemented.
 * <p/>
 * If the Pojo doesn't implement the this interface, the {@link PojoSlice} will decide what to do.
 *
 * @author Peter Veentjer.
 */
public interface HazelcastInstanceProvider {

    /**
     * Gets the {@link HazelcastInstance}.
     *
     * @return the provided HazelcastInstance, or null if the Pojo doesn't want to be responsible for providing
     *         the HazelcastInstance.
     */
    HazelcastInstance getHazelcastInstance();
}
