package com.hazelblast.server;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;

/**
 * To make a distributed system, a verical slice (so top to bottom) can be placed in parallel. Each machine can be
 * responsible for running a vertical complete slice, just add more machines for more capacity.
 * <p/>
 * A Slice has lifecycle hooks that are called when it is started and stopped, but also when a partition
 * is removed or added from this node.
 * <p/>
 * In Spring terminology it would be called an ApplicationContext.
 *
 * The methods {@link #onStart()},{@link #onStop()}, {@link #onPartitionAdded(int)} and {@link #onPartitionRemoved(int)}}
 * will never be called concurrently. This is guaranteed.
 *
 * @author Peter Veentjer.
 */
public interface Slice {

    String DEFAULT_NAME = "default";

    /**
     * Returns the name of the Slice.
     *
     * @return the name of the Slice.
     */
    String getName();

    /**
     * Returns the {@link HazelcastInstance} this Slice makes use of. Value will be constant and never be null.
     *
     * @return the HazelcastInstance.
     */
    HazelcastInstance getHazelcastInstance();

    /**
     * Gets the Service with the given serviceName.
     *
     * @param serviceName the serviceName of the service to look for.
     * @return the found service.
     * @throws RuntimeException when service doesn't exist or failed to be constructed. Type of RuntimeException
     *                          depends on framework being used.
     */
    Object getService(String serviceName);

    /**
     * Called when the {@link Slice} is started.
     * <p/>
     * This method will be called only once on an instance.
     */
    void onStart();

    /**
     * Called when the {@link Slice} is stopped.
     * <p/>
     * This method will be called only once on an instance.
     */
    void onStop();

    /**
     * Called when a partition is added to this {@link Slice}.
     *
     * @param partitionId the id of the partition.
     */
    void onPartitionAdded(int partitionId);

    /**
     * Called when a partition is removed from this {@link Slice}.
     *
     * @param partitionId the id of this partition.
     */
    void onPartitionRemoved(int partitionId);
}
