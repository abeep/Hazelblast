package com.hazelblast.server.spring;

/**
 * A callback interface that any bean in a Spring applicationcontext can implement, so that it will be notified when
 * partitions are added or removed.
 *
 * @author Peter Veentjer.
 */
public interface SpringPartitionListener {

    /**
     * Called when a partition is added.
     *
     * @param partitionId the id of the partition that is added.
     */
    void onPartitionAdded(int partitionId);

    /**
     * Called when a partition is removed.
     *
     * @param partitionId the id of the partition that is removed.
     */
    void onPartitionRemoved(int partitionId);
}
