package com.hazelblast.server;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;

import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;

/**
 * The PuMonitor is responsible for seeing changes in the Hazelcast partitions, and to notify these changes
 * back to the PuContainer.
 * <p/>
 * There is a  {@link #scan()} method that is called externally by some scheduler, so no threading inside this
 * structure.
 *
 * @author Peter Veentjer.
 */
public final class PuMonitor {

    private final static ILogger logger = Logger.getLogger(PuMonitor.class.getName());

    private final PuContainer puContainer;
    private final Member self = Hazelcast.getCluster().getLocalMember();
    private final PartitionService partitionService = Hazelcast.getPartitionService();

    /**
     * Creates a new PuMonitor.
     *
     * @param puContainer the puContainer that is signalled by this PuMonitor.
     * @throws NullPointerException if puContainer is null.
     */
    public PuMonitor(PuContainer puContainer) {
        if (puContainer == null) {
            throw new NullPointerException("puContainer can't be null");
        }
        this.puContainer = puContainer;
    }

    /**
     * Executes a scan. This method should be called by some kind of Scheduler.
     */
    public void scan() {
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, "scan");
        }

        Set<Partition> partitions = partitionService.getPartitions();

        boolean change = false;

        for (Partition p : partitions) {
            int partitionId = p.getPartitionId();
            if (p.getOwner().equals(self)) {
                boolean addPu = !puContainer.containsPartition(partitionId);
                if (addPu) {
                    Lock lock = Hazelcast.getLock("PartitionLock-" + partitionId);
                    if (!lock.tryLock()) {
                        if (logger.isLoggable(Level.FINE)) {
                            logger.log(Level.FINE, "Could not obtain lock on partition " + partitionId + ", maybe more luck next time.");
                        }

                        break;
                    }

                    change = true;
                    puContainer.onPartitionAdded(partitionId);
                }
            } else {
                boolean removePu = puContainer.containsPartition(partitionId);
                if (removePu) {
                    puContainer.onPartitionRemoved(partitionId);

                    Lock lock = Hazelcast.getLock("PartitionLock-" + partitionId);
                    lock.unlock();

                    change = true;
                }
            }
        }

        if (change) {
            logger.log(Level.INFO, "Scan complete, managed partitions: " + puContainer.getPartitionCount());
        }
    }
}
