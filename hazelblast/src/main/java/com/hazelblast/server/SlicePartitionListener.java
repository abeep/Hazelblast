package com.hazelblast.server;

import com.hazelcast.partition.Partition;

/**
 * A callback interface that can be implemented to get notified of partition changes in a {@link Slice}.
 *
 * @author Peter Veentjer.
 */
public interface SlicePartitionListener {

    /**
     * Called when a partition is added to the {@link Slice}.
     *
     * @param partition the Partition that was added.
     */
    void onPartitionAdded(Partition partition);

    /**
     * Called when a partition is removed from the {@link Slice}.
     *
     * @param partition the Partition that was removed.
     */
    void onPartitionRemoved(Partition partition);
}
