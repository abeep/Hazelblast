package com.hazelblast.server;

import com.hazelblast.server.pojo.PojoUtils;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
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
    private final Set<Integer> managedPartitions = Collections.synchronizedSet(new HashSet<Integer>());
    private final String serviceContextName;

    private final PartitionService partitionService;
    private final Member self;
    private final Map<Integer, ILock> partitionLockMap = new HashMap<Integer, ILock>();
    private final List<Partition> partitions = new ArrayList<Partition>();

    /**
     * Creates a new ServiceContextContainer with the given ServiceContext.
     *
     * @param serviceContext     the ServiceContext contained in this ServiceContextContainer.
     * @param serviceContextName the name of the ServiceContext
     * @throws NullPointerException if serviceContext or serviceContextName is null.
     */
    public ServiceContextContainer(ServiceContext serviceContext, String serviceContextName, HazelcastInstance hazelcastInstance) {
        this.serviceContext = notNull("serviceContext", serviceContext);
        this.serviceContextName = notNull("serviceContextName", serviceContextName);
        notNull("hazelcastInstance", hazelcastInstance);

        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, format("Created ServiceContextContainer containing ServiceContext [%s]", serviceContext));
        }

        self = hazelcastInstance.getCluster().getLocalMember();
        partitionService = hazelcastInstance.getPartitionService();
        for (Partition partition : partitionService.getPartitions()) {
            int partitionId = partition.getPartitionId();
            ILock lock = hazelcastInstance.getLock("PartitionLock-" + partitionId);
            partitionLockMap.put(partitionId, lock);
            partitions.add(partition);
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
    protected boolean containsPartition(int partitionId) {
        return managedPartitions.contains(partitionId);
    }

    /**
     * Gets the number of partitions that are part of this ServiceContextContainer.
     *
     * @return the number of partitions.
     */
    protected int getPartitionCount() {
        return managedPartitions.size();
    }

    public Object executeMethod(String serviceName, String methodName, String[] argTypes, Object[] args, Object partitionKey) throws Throwable {
        notNull("serviceContextName", serviceContextName);
        notNull("serviceName", serviceName);
        notNull("methodName", methodName);
        notNull("args", args);

        //The first thing that needs to be checked, is if the partition that was expected to be here when the call
        //was send to this machine, is still there. If it isn't, some kind of exception should be thrown, this exception
        //should be caught by the proxy and the method call should be retried, now hoping that

        if (partitionKey != null) {
            Partition partition = partitionService.getPartition(partitionKey);

            if (!managedPartitions.contains(partition.getPartitionId())) {
                //if the partition is not managed by this servicecontextcontainer, we throw an exception that
                //will be caught by the proxy, and the call will be retried.
                //throw new PartitionMovedException();
            }

            //ISemaphore lock = partitionLockMap.get(partition.getPartitionId());
            //if (lock.availablePermits() == 1) {
            //    //the pa
            //}
        }


        Object service = serviceContext.getService(serviceName);

        Class serviceClass = service.getClass();
        Method[] methods = serviceClass.getMethods();
        Method foundMethod = null;
        for (Method method : methods) {
            if (PojoUtils.matches(method, methodName, argTypes)) {
                foundMethod = method;
                break;
            }
        }

        if (foundMethod == null) {
            //todo; better exception
            throw new IllegalStateException();
        }

        try {
            return foundMethod.invoke(service, args);
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }


    /**
     * Executes a scanForPartitionChanges; so checks the partition table to see if there are change.
     * <p/>
     * This method should be called by some kind of Scheduler.   It is not threadsafe,
     * and should always be called by the same thread (because locks are used and they need to be unlocked by
     * the same thread that acquired the lock).
     */
    public void scanForPartitionChanges() {
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, format("[%s] Scan", serviceContextName));
        }

        boolean changeDetected = false;

        for (Partition partition : partitions) {
            int partitionId = partition.getPartitionId();
            if (partition.getOwner().equals(self)) {
                boolean startManagingPartition = !managedPartitions.contains(partitionId);

                if (startManagingPartition) {
                    ILock lock = partitionLockMap.get(partitionId);

                    if (!lock.tryLock()) {
                        if (logger.isLoggable(Level.FINE)) {
                            logger.log(Level.FINE, format("[%s] Could not obtain lock on partition [%s], maybe more luck next time.",
                                    serviceContextName, partitionId));
                        }
                    } else {
                        changeDetected = true;

                        try {
                            serviceContext.onPartitionAdded(partitionId);
                        } catch (Exception e) {
                            logger.log(Level.SEVERE, format("[%s] Failed to execute ServiceContext.OnPartitionAdded", serviceContextName), e);
                        }

                        //by adding the partition to the managed partitions, external calls are allowed to be executed again.
                        managedPartitions.add(partitionId);
                    }
                }
            } else {
                boolean stopManagingPartition = managedPartitions.contains(partitionId);

                //todo; we should now wait till all threads have returned that are calling services.

                if (stopManagingPartition) {
                    //removing the partition from the managedPartitions, prevents new calls from being accepted.
                    managedPartitions.remove(partitionId);

                    //first we give the container the chance to terminate/persist all resources that were available
                    //for the given partition.
                    try {
                        serviceContext.onPartitionRemoved(partitionId);
                    } catch (Exception e) {
                        logger.log(Level.SEVERE, format("[%s] Failed to execute ServiceContext.OnPartitionAdded", serviceContextName), e);
                    }

                    //we release the lock, so that a different node now is able to take over the partition.
                    ILock lock = partitionLockMap.get(partitionId);
                    lock.unlock();

                    changeDetected = true;
                }
            }
        }

        if (changeDetected && logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, format("[%s] Scan complete, managed partitions [%s] ",
                    serviceContextName, getPartitionCount()));
        }
    }
}
