package com.hazelblast.server;

import com.hazelblast.api.ProcessingUnit;
import com.hazelblast.api.PuFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import static java.lang.String.format;

/**
 * The container that runs the {@link ProcessingUnit} and manages partitions (although Hazelcast does the
 * real management).
 * <p/>
 * In the future it could be that multiple processing units are hosted in the same container, but for the
 * time being it is only one.
 * <p/>
 * The PuContainer expects a puFactory System property. This puFactory should point to a class of type
 * {@link PuFactory} with no arg constructor. This factory will be used to construct the processing unit.
 *
 * @author Peter Veentjer.
 */
public class PuContainer {

    public static final String PU_FACTORY_CLASS = "puFactory.class";

    //todo: this instance sucks.
    public static volatile PuContainer instance;

    private final static ILogger logger = Logger.getLogger(PuContainer.class.getName());

    private final ProcessingUnit pu;
    private final ConcurrentMap<Integer, Object> partitionMap = new ConcurrentHashMap<Integer, Object>();

    /**
     * Creates a new PuContainer with the given ProcessingUnit.
     *
     * @param pu the ProcessingUnit contained in this PuContainer.
     * @throws NullPointerException if pu is null.
     */
    public PuContainer(ProcessingUnit pu) {
        if (pu == null) {
            throw new NullPointerException();
        }
        this.pu = pu;

        if(logger.isLoggable(Level.INFO)){
            logger.log(Level.INFO,format("Created PuContainer using ProcessingUnit [%s]",pu));
        }
    }

    public PuContainer() {
        String factoryName = System.getProperty(PU_FACTORY_CLASS);

        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, format("Creating PuContainer using System property %s and value %s", PU_FACTORY_CLASS, factoryName));
        }

        if (factoryName == null) {
            throw new IllegalStateException(format("Property [%s] is not found in the System properties",PU_FACTORY_CLASS));
        }

        ClassLoader classLoader = PuContainer.class.getClassLoader();
        try {
            Class<PuFactory> factoryClazz = (Class<PuFactory>) classLoader.loadClass(factoryName);
            PuFactory puFactory = factoryClazz.newInstance();
            pu = puFactory.create();
            pu.onStart();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(format("Failed to create processing unit using System property %s " + factoryName,PU_FACTORY_CLASS), e);
        } catch (InstantiationException e) {
            throw new RuntimeException("Failed to create processing unit using puFactor.class " + factoryName, e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to create processing unit using puFactor.class " + factoryName, e);
        }

        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, "Creating PuContainer successfully");
        }
    }

    /**
     * Gets the processing unit that is contained in this Container.
     *
     * @return the processing unit.
     */
    public ProcessingUnit getPu() {
        return pu;
    }

    /**
     * Called when a partition has been added.
     * <p/>
     * This call should only be made by the PuMonitor.
     *
     * @param partitionId the id of the partition that has been added.
     */
    public void onPartitionAdded(int partitionId) {
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, "onPartitionAdded" + partitionId);
        }

        if (partitionMap.containsKey(partitionId)) {
            throw new IllegalArgumentException("Partition " + partitionId + " already exists");
        }

        partitionMap.put(partitionId, partitionId);

        try {
            pu.onPartitionAdded(partitionId);
        } catch (RuntimeException e) {
            logger.log(Level.SEVERE, "failed to execute pu.OnPartitionAdded", e);
        }
    }

    /**
     * Called when a partition has been removed.
     * <p/>
     * This call should only be made by the PuMonitor.
     *
     * @param partitionId the id of the partition that has been removed.
     */
    public void onPartitionRemoved(int partitionId) {
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, "onPartitionRemoved" + partitionId);
        }

        boolean changed = partitionMap.remove(partitionId, partitionId);

        if (!changed) {
            throw new IllegalArgumentException("Partition " + partitionId + " doesn't exist");
        }

        try {
            pu.onPartitionRemoved(partitionId);
        } catch (RuntimeException e) {
            logger.log(Level.SEVERE, "failed to execute pu.OnPartitionAdded", e);
        }
    }

    /**
     * Checks if a partition with the given id is part of this PuContainer.
     *
     * @param partitionId the id of the partition.
     * @return true if the partition is part of this PuContainer, false otherwise.
     */
    public boolean containsPartition(int partitionId) {
        return partitionMap.containsKey(partitionId);
    }

    /**
     * Gets the number of partitions that are part of this PuContainer.
     *
     * @return the number of partitions.
     */
    public int getPartitionCount() {
        return partitionMap.size();
    }
}
