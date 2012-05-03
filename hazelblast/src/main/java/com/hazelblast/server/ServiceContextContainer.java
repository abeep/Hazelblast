package com.hazelblast.server;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import static com.hazelblast.utils.Arguments.notNull;
import static java.lang.String.format;

/**
 * The container that runs the {@link ServiceContext}.
 *
 * @author Peter Veentjer.
 */
final class ServiceContextContainer {

    private final static ILogger logger = Logger.getLogger(ServiceContextContainer.class.getName());

    private final ServiceContext serviceContext;
    private final ConcurrentMap<Integer, Object> partitionMap = new ConcurrentHashMap<Integer, Object>();
    private final String serviceContextName;

    /**
     * Creates a new ServiceContextContainer with the given ServiceContext.
     *
     * @param serviceContext     the ServiceContext contained in this ServiceContextContainer.
     * @param serviceContextName the name of the ServiceContext
     * @throws NullPointerException if serviceContext or serviceContextName is null.
     */
    public ServiceContextContainer(ServiceContext serviceContext, String serviceContextName) {
        this.serviceContext = notNull("serviceContext",serviceContext);
        this.serviceContextName = notNull("serviceContextName",serviceContextName);

        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, format("Created ServiceContextContainer containing ServiceContext [%s]", serviceContext));
        }
    }

    /**
     * Returns the name of the serviceContext.
     *
     * @return the name of the ServiceContext.
     */
    public String getServiceContextName() {
        return serviceContextName;
    }

    /**
     * Gets the {@link ServiceContext} that is contained in this Container.
     *
     * @return the {@link ServiceContext}.
     */
    public ServiceContext getServiceContext() {
        return serviceContext;
    }

    /**
     * Called when a partition has been added.
     * <p/>
     * This call should only be made by the PartitionMonitor.
     *
     * @param partitionId the id of the partition that has been added.
     */
    public void onPartitionAdded(int partitionId) {
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, format("[%s] onPartitionAdded [%s]", serviceContextName, partitionId));
        }

        if (partitionMap.containsKey(partitionId)) {
            throw new IllegalArgumentException("Partition " + partitionId + " already exists in serviceContext " + serviceContextName);
        }

        partitionMap.put(partitionId, partitionId);

        try {
            serviceContext.onPartitionAdded(partitionId);
        } catch (RuntimeException e) {
            logger.log(Level.SEVERE, format("[%s] Failed to execute serviceContext.OnPartitionAdded", serviceContextName), e);
        }
    }

    /**
     * Called when a partition has been removed.
     * <p/>
     * This call should only be made by the PartitionMonitor.
     *
     * @param partitionId the id of the partition that has been removed.
     */
    public void onPartitionRemoved(int partitionId) {
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, format("[%s] onPartitionRemoved [%s]", serviceContextName, partitionId));
        }

        boolean changed = partitionMap.remove(partitionId, partitionId);

        if (!changed) {
            throw new IllegalArgumentException("Partition " + partitionId + " doesn't exist in serviceContext " + serviceContextName);
        }

        try {
            serviceContext.onPartitionRemoved(partitionId);
        } catch (RuntimeException e) {
            logger.log(Level.SEVERE, format("[%s] Failed to execute serviceContext.OnPartitionAdded", serviceContextName), e);
        }
    }


    /**
     * Called when the ServiceContextContainer needs to start up.
     * <p/>
     * If the {@link ServiceContext#onStart()} throws an Exception, it will be logged but not
     * propagated.
     */
    public void onStart() {
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, format("[%s] onStart called", serviceContextName));
        }

        try {
            serviceContext.onStart();
        } catch (Exception e) {
            logger.log(Level.SEVERE, format("[%s] Failed to execute serviceContext.onStart", serviceContextName), e);
        }
    }

    /**
     * Called when the ServiceContextContainer needs to shut down.
     * <p/>
     * If the {@link ServiceContext#onStop()} throws an Exception, it will be logged but not
     * propagated.
     */
    public void onStop() {
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, format("[%s] onStop called", serviceContextName));
        }

        try {
            serviceContext.onStop();
        } catch (Exception e) {
            logger.log(Level.SEVERE, format("[%s] Failed to execute serviceContext.onStop", serviceContextName), e);
        }
    }

    /**
     * Checks if a partition with the given id is part of this ServiceContextContainer.
     *
     * @param partitionId the id of the partition.
     * @return true if the partition is part of this ServiceContextContainer, false otherwise.
     */
    public boolean containsPartition(int partitionId) {
        return partitionMap.containsKey(partitionId);
    }

    /**
     * Gets the number of partitions that are part of this ServiceContextContainer.
     *
     * @return the number of partitions.
     */
    public int getPartitionCount() {
        return partitionMap.size();
    }
}
