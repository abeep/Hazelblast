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

import static java.lang.String.format;

/**
 * The PartitionMonitor is responsible for seeing changes in the Hazelcast partitions, and to notify these changes
 * back to the ServiceContextContainer.
 * <p/>
 * There is a  {@link #scan()} method that is called externally by some scheduler, so no threading inside this
 * structure.
 *
 * @author Peter Veentjer.
 */
final class PartitionMonitor {

    private final static ILogger logger = Logger.getLogger(PartitionMonitor.class.getName());

    private final ServiceContextContainer serviceContextContainer;
    private final Member self = Hazelcast.getCluster().getLocalMember();
    private final PartitionService partitionService = Hazelcast.getPartitionService();

    /**
     * Creates a new PartitionMonitor.
     *
     * @param serviceContextContainer the serviceContextContainer that is signalled by this PartitionMonitor.
     * @throws NullPointerException if serviceContextContainer is null.
     */
    public PartitionMonitor(ServiceContextContainer serviceContextContainer) {
        if (serviceContextContainer == null) {
            throw new NullPointerException("serviceContextContainer can't be null");
        }
        this.serviceContextContainer = serviceContextContainer;
    }

    /**
     * Executes a scan. This method should be called by some kind of Scheduler.
     */
    public void scan() {
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, format("[%s] Scan", serviceContextContainer.getServiceContextName()));
        }

        Set<Partition> partitions = partitionService.getPartitions();

        boolean change = false;

        for (Partition p : partitions) {
            int partitionId = p.getPartitionId();
            if (p.getOwner().equals(self)) {
                boolean addPu = !serviceContextContainer.containsPartition(partitionId);
                if (addPu) {
                    Lock lock = Hazelcast.getLock("PartitionLock-" + partitionId);
                    if (!lock.tryLock()) {
                        if (logger.isLoggable(Level.FINE)) {
                            logger.log(Level.FINE, format("[%s] Could not obtain lock on partition [%s], maybe more luck next time.", serviceContextContainer.getServiceContextName(),partitionId));
                        }

                        break;
                    }

                    change = true;
                    serviceContextContainer.onPartitionAdded(partitionId);
                }
            } else {
                boolean removePu = serviceContextContainer.containsPartition(partitionId);
                if (removePu) {
                    serviceContextContainer.onPartitionRemoved(partitionId);

                    Lock lock = Hazelcast.getLock("PartitionLock-" + partitionId);
                    lock.unlock();

                    change = true;
                }
            }
        }

        if (change) {
            logger.log(Level.INFO, format("[%s] Scan complete, managed partitions [%s] ", serviceContextContainer.getServiceContextName(), serviceContextContainer.getPartitionCount()));
        }
    }
}
