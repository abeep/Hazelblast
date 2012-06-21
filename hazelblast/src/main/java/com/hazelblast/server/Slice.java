package com.hazelblast.server;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.Partition;

/**
 * To make a distributed system, a vertical slice (so top to bottom) can be placed in parallel. Each machine can be
 * responsible for running a complete vertical complete and you can just add more machines for more capacity.
 * <p/>
 * In Spring terminology this could be compared to an application-context.
 *
 * A Slice has lifecycle hooks that are called when it is started and stopped and when partitions are added/removed
 * from the slice.
 * <p/>
 * The following lifecycle methods will never be called concurrently:
 * <ol>
 *     <li>{@link #onStart()}</li>
 *     <li>{@link #onStop()}</li>
 *     <li>{@link #onPartitionAdded(Partition)}</li>
 *     <li>{@link #onPartitionRemoved(Partition)}}</li>
 * </ol>
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
     * @param partition the {@link Partition}.
     */
    void onPartitionAdded(Partition partition);

    /**
     * Called when a partition is removed from this {@link Slice}.
     *
     * @param partition the {@link Partition}
     */
    void onPartitionRemoved(Partition partition);
}
