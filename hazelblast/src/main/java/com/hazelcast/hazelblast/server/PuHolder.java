package com.hazelcast.hazelblast.server;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.hazelblast.api.ProcessingUnit;

import java.util.logging.Level;

public class PuHolder {
    private final static ILogger logger = Logger.getLogger(PuHolder.class.getName());

    private final int partitionId;
    private final ProcessingUnit pu;

    public PuHolder(int partitionId, ProcessingUnit pu) {
        this.partitionId = partitionId;
        this.pu = pu;
    }

    public ProcessingUnit getPu() {
        return pu;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void start() {
        logger.log(Level.INFO, "Processing unit started: " + partitionId);

        try {
            pu.start();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to call start on processing unit", e);
        }
    }

    public void stop() {
        logger.log(Level.INFO, "Processing unit stopped: " + partitionId);

        try {
            pu.stop();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to call stop on processing unit", e);
        }
    }

    @Override
    public String toString() {
        return "PuHolder{" +
                "partitionId=" + partitionId +
                '}';
    }
}
