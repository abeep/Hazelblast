package com.hazelblast.server;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;

public class PuMonitor {

    private final static ILogger logger = Logger.getLogger(PuMonitor.class.getName());

    private final PuContainer puContainer;
    private final Member self = Hazelcast.getCluster().getLocalMember();
    private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);
    private final PartitionService partitionService = Hazelcast.getPartitionService();

    public PuMonitor(PuContainer puContainer) {
        this.puContainer = puContainer;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                try {
                    scan();
                } catch (RuntimeException e) {
                    logger.log(Level.SEVERE, "Failed to run PuMontitor.scan", e);
                }
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    private void scan() {
        Set<Partition> partitions = partitionService.getPartitions();

        boolean change = false;

        for (Partition p : partitions) {
            int partitionId = p.getPartitionId();
            if (p.getOwner().equals(self)) {
                boolean addPu = !puContainer.containsKey(partitionId);
                if (addPu) {
                    Lock lock = Hazelcast.getLock("PartitionLock-" + partitionId);
                    if (!lock.tryLock()) {
                        break;
                    }

                    change = true;
                    puContainer.startPu(partitionId);
                }
            } else {
                boolean removePu = puContainer.containsKey(partitionId);
                if (removePu) {
                    puContainer.terminatePu(partitionId);

                    Lock lock = Hazelcast.getLock("PartitionLock-" + partitionId);
                    lock.unlock();

                    change = true;
                }
            }
        }

        if (change) {
            logger.log(Level.INFO, "managed pu's: " + puContainer.size());
        }
    }
}
