package com.hazelblast.api;

/**
 * The ServiceContext is an place where all kinds of Services can be exposed to the outside world.
 *
 * @author Peter Veentjer.
 */
public interface ServiceContext {

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
     * Called when the {@link ServiceContext} is started.
     * <p/>
     * This method will be called only once on an instance.
     */
    void onStart();

    /**
     * Called when the {@link ServiceContext} is stopped.
     * <p/>
     * This method will be called only once on an instance.
     */
    void onStop();

    /**
     * Called when a partition is added to this {@link ServiceContext}.
     *
     * @param partitionId the id of the partition.
     */
    void onPartitionAdded(int partitionId);

    /**
     * Called when a partition is removed from this {@link ServiceContext}.
     *
     * @param partitionId the id of this partition.
     */
    void onPartitionRemoved(int partitionId);
}
