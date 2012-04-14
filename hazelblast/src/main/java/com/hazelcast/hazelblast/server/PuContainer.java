package com.hazelcast.hazelblast.server;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.hazelblast.api.ProcessingUnit;
import com.hazelcast.hazelblast.api.PuFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

public class PuContainer {

    private final static ILogger logger = Logger.getLogger(PuContainer.class.getName());

    public final static PuContainer INSTANCE = new PuContainer();

    private final ConcurrentMap<Integer, PuHolder> puHolders = new ConcurrentHashMap<Integer, PuHolder>();
    private final PuFactory puFactory;

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
            puFactory = factoryClazz.newInstance();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public PuHolder getHolder(int partitionId) {
        return puHolders.get(partitionId);
    }

    public void startPu(int partitionId) {
        if (puHolders.containsKey(partitionId)) {
            throw new IllegalStateException("There already is an existing pu with partition-id " + partitionId);
        }

        ProcessingUnit pu = puFactory.create(partitionId);
        PuHolder puHolder = new PuHolder(partitionId, pu);
        puHolders.put(partitionId, puHolder);
        puHolder.start();
    }

    public void terminatePu(int partitionId) {
        PuHolder puHolder = puHolders.remove(partitionId);
        if (puHolder == null) {
            throw new IllegalStateException("No pu found with partition-id " + partitionId);
        }
        puHolder.stop();
    }

    public boolean containsKey(int partitionId) {
        return puHolders.containsKey(partitionId);
    }

    public int size() {
        return puHolders.size();
    }
}
