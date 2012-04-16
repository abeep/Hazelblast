package com.hazelblast.api;

/**
 * The ProcessingUnit is an place where all kinds of Services can be exposed to the outside world.
 *
 * @author Peter Veentjer.
 */
public interface ProcessingUnit {

    /**
     * Gets the Service with the given serviceName.
     *
     * @param serviceName the serviceName of the service to look for.
     * @return the found service, or null if the Service doesn't exist.
     */
    Object getService(String serviceName);

    /**
     * Called when the processing unit needs to be started.
     * <p/>
     * This method will be called only once on an instance.
     */
    void onStart();

    /**
     * Called when processing unit needs to be stopped.
     * <p/>
     * This method will be called only once on an instance.
     */
    void onStop();

    /**
     * Called when a partition is added to the processing unit.
     *
     * @param partitionId the id of the partition.
     */
    void onPartitionAdded(int partitionId);

    /**
     * Called when a partition is removed from a processing unit.
     *
     * @param partitionId the id of this partition.
     */
    void onPartitionRemoved(int partitionId);
}
