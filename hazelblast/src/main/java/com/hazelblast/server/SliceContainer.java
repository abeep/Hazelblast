package com.hazelblast.server;

import com.hazelblast.server.exceptions.PartitionMovedException;
import com.hazelblast.server.pojoslice.PojoUtils;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.logging.Level;

import static com.hazelblast.utils.Arguments.notNull;
import static java.lang.String.format;

/**
 * The container that runs the {@link Slice}.
 *
 * @author Peter Veentjer.
 */
final class SliceContainer {

    private final ILogger logger;

    private final Slice slice;
    private final Set<Integer> managedPartitions = Collections.synchronizedSet(new HashSet<Integer>());
    private final String sliceName;

    private final PartitionService partitionService;
    private final Member self;
    private final Map<Integer, ILock> partitionLockMap = new HashMap<Integer, ILock>();
    private final List<Partition> partitions = new ArrayList<Partition>();

    /**
     * Creates a new SliceContainer with the given Slice.
     *
     * @param slice     the Slice contained in this SliceContainer.
     * @param sliceName the name of the Slice
     * @throws NullPointerException if slice or sliceName is null.
     */
    public SliceContainer(Slice slice, String sliceName, HazelcastInstance hazelcastInstance) {
        this.slice = notNull("slice", slice);
        this.sliceName = notNull("sliceName", sliceName);
        notNull("hazelcastInstance", hazelcastInstance);
        this.logger = hazelcastInstance.getLoggingService().getLogger(SliceContainer.class.getName());

        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, format("Created SliceContainer containing Slice [%s]", slice));
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
     * Called when the SliceContainer needs to start up.
     * <p/>
     * If the {@link Slice#onStart()} throws an Exception, it will be logged but not
     * propagated.
     */
    public void start() {
        long startMs = System.currentTimeMillis();

        if (logger.isLoggable(Level.FINEST)) {
            logger.log(Level.FINEST, format("[%s] Slice.onStart() begin", sliceName));
        }

        try {
            slice.onStart();

            if (logger.isLoggable(Level.INFO)) {
                long durationMs = System.currentTimeMillis() - startMs;
                logger.log(Level.INFO, format("[%s] Slice.onStart() finished in [%s] ms", sliceName, durationMs));
            }
        } catch (Throwable e) {
            logger.log(Level.SEVERE, format("[%s] Slice.onStart() failed", sliceName), e);
        }
    }

    /**
     * Called when the SliceContainer needs to shut down.
     * <p/>
     * If the {@link Slice#onStop()} throws an Exception, it will be logged but not
     * propagated.
     */
    public void stop() {
        long startMs = System.currentTimeMillis();

        if (logger.isLoggable(Level.FINEST)) {
            logger.log(Level.FINEST, format("[%s] Slice.onStop() begin", sliceName));
        }

        try {
            slice.onStop();

            if (logger.isLoggable(Level.INFO)) {
                long durationMs = System.currentTimeMillis() - startMs;
                logger.log(Level.INFO, format("[%s] Slice.onStop() finished in [%s] ms", sliceName, durationMs));
            }
        } catch (Throwable e) {
            logger.log(Level.SEVERE, format("[%s] Slice.onStop() failed", sliceName), e);
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
        long startMs = System.currentTimeMillis();

        if (logger.isLoggable(Level.FINEST)) {
            logger.log(Level.FINEST, format("[%s] Scan started", sliceName));
        }

        boolean changeDetected = false;

        for (Partition partition : partitions) {
            int partitionId = partition.getPartitionId();
            if (self.equals(partition.getOwner())) {
                boolean startManagingPartition = !managedPartitions.contains(partitionId);

                if (startManagingPartition) {
                    if (addPartition(partitionId)) {
                        changeDetected = true;
                    }
                }
            } else {
                boolean stopManagingPartition = managedPartitions.contains(partitionId);

                if (stopManagingPartition) {
                    removePartition(partitionId);
                    changeDetected = true;
                }
            }
        }

        if (changeDetected && logger.isLoggable(Level.INFO)) {
            long durationMs = System.currentTimeMillis() - startMs;
            logger.log(Level.INFO, format("[%s] Scan complete, managed partitions [%s], total time [%s] ms",
                    sliceName, managedPartitions.size(), durationMs));
        }
    }

    private void removePartition(int partitionId) {
        //removing the partition from the managedPartitions, prevents new calls from being accepted.
        managedPartitions.remove(partitionId);

        long startMs = System.currentTimeMillis();
        if (logger.isLoggable(Level.FINEST)) {
            logger.log(Level.FINEST, format("[%s] Slice.onPartitionRemoved(%s) begin", sliceName, partitionId));
        }

        //first we give the container the chance to terminate/persist all resources that were available
        //for the given partition.
        try {
            slice.onPartitionRemoved(partitionId);

            if (logger.isLoggable(Level.FINEST)) {
                long durationMs = System.currentTimeMillis() - startMs;
                logger.log(Level.FINEST, format("[%s] Slice.onPartitionRemoved(%s) finished in [%s] ms", sliceName, partitionId, durationMs));
            }
        } catch (Throwable e) {
            logger.log(Level.SEVERE, format("[%s] Slice.onPartitionRemoved(%s) failed", sliceName, partitionId), e);
        }

        //we release the lock, so that a different node now is able to take over the partition.
        ILock lock = partitionLockMap.get(partitionId);
        lock.unlock();
    }

    private boolean addPartition(int partitionId) {
        boolean changeDetected = false;

        ILock lock = partitionLockMap.get(partitionId);

        if (!lock.tryLock()) {
            if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST, format("[%s] Could not obtain lock on partition [%s], maybe more luck next time.",
                        sliceName, partitionId));
            }
        } else {
            changeDetected = true;

            long startMs = System.currentTimeMillis();
            if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST, format("[%s] Slice.onPartitionAdded(%s) begin", sliceName, partitionId));
            }

            try {
                slice.onPartitionAdded(partitionId);

                if (logger.isLoggable(Level.FINEST)) {
                    long durationMs = System.currentTimeMillis() - startMs;
                    logger.log(Level.FINEST, format("[%s] Slice.onPartitionAdded(%s) finished in [%s] ms", sliceName, partitionId, durationMs));
                }
            } catch (Throwable e) {
                logger.log(Level.SEVERE, format("[%s] Slice.OnPartitionAdded(%s) failed", sliceName, partitionId), e);
            }

            //by adding the partition to the managed partitions, external calls are allowed to be executed again.
            managedPartitions.add(partitionId);
        }
        return changeDetected;
    }

    public Object executeMethod(String serviceName, String methodName, String[] argTypes, Object[] args, Object partitionKey) throws Throwable {
        notNull("serviceName", serviceName);
        notNull("methodName", methodName);

        //todo: logging of method under finest

        //The first thing that needs to be checked, is if the partition that was expected to be here when the call
        //was send to this machine, is still there. If it isn't, some kind of exception should be thrown, this exception
        //should be caught by the proxy and the method call should be retried, now hoping that

        if (partitionKey != null) {
            Partition partition = partitionService.getPartition(partitionKey);

            int partitionId = partition.getPartitionId();
            if (!managedPartitions.contains(partitionId)) {
                //if the partition is not managed by this SliceContainer, we throw an exception that
                //will be caught by the proxy, and the call will be retried.
                throw new PartitionMovedException(format("Partition [%s] is not found on member [%s]", partitionId, self));
            }
        }

        Object service = slice.getService(serviceName);

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
}
