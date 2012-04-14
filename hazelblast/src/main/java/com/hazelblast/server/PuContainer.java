package com.hazelblast.server;

import com.hazelblast.api.ProcessingUnit;
import com.hazelblast.api.PuFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

public class PuContainer {

    private final static ILogger logger = Logger.getLogger(PuContainer.class.getName());

    public final static PuContainer INSTANCE = new PuContainer();

    private final ProcessingUnit pu;
    private final ConcurrentMap<Integer, Object> partitionMap = new ConcurrentHashMap<Integer, Object>();

    private PuContainer() {
        String factoryName = System.getProperty("puFactory");

        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, "Using puFactory: " + factoryName);
        }

        if (factoryName == null) {
            throw new IllegalStateException("property [puFactory] is not found in the System properties");
        }

        ClassLoader classLoader = PuContainer.class.getClassLoader();
        try {
            Class<PuFactory> factoryClazz = (Class<PuFactory>) classLoader.loadClass(factoryName);
            PuFactory puFactory = factoryClazz.newInstance();
            pu = puFactory.create();
            pu.onStart();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public ProcessingUnit getPu() {
        return pu;
    }

    public void onPartitionAdded(int partitionId) {
        partitionMap.put(partitionId, partitionId);

        try {
            pu.onPartitionAdded(partitionId);
        } catch (RuntimeException e) {
            logger.log(Level.SEVERE, "failed to execute pu.OnPartitionAdded", e);
        }
    }

    public void pnPartitionRemoved(int partitionId) {
        partitionMap.remove(partitionId, partitionId);

        try {
            pu.onPartitionRemoved(partitionId);
        } catch (RuntimeException e) {
            logger.log(Level.SEVERE, "failed to execute pu.OnPartitionAdded", e);
        }
    }

    public boolean containsPartition(int partitionId) {
        return partitionMap.containsKey(partitionId);
    }

    public int getPartitionCount() {
        return partitionMap.size();
    }
}
