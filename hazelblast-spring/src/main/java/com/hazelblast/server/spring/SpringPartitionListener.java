package com.hazelblast.server.spring;

/**
 * A callback interface that any bean in the application context can implement, so that it will be notified when
 * partitions are added or removed.
 *
 * @author Peter Veentjer.
 */
public interface SpringPartitionListener {

    void onPartitionAdded(int partitionId);

    void onPartitionRemoved(int partitionId);
}
