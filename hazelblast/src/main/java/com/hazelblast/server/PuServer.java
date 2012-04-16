package com.hazelblast.server;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

/**
 * The PuServer is responsible for hosting the {@link PuContainer} and can either run
 * standalone, or can be embedded in an existing java application.
 * <p/>
 * TODO: There is a bug in getting to the terminated state when this PuServer is running.
 *
 * @author Peter Veentjer.
 */
public class PuServer {
    public final static int DEFAULT_SCAN_DELAY_MS = 5000;

    private final static ILogger logger = Logger.getLogger(PuServer.class.getName());

    public static void main(String[] args) {
        PuServer main = new PuServer();
        main.start();
    }

    protected enum Status {Unstarted, Running, Terminating, Terminated}

    private final PuMonitor puMonitor;
    private final PuContainer puContainer;
    private final ScheduledThreadPoolExecutor scheduler;
    private final Lock stateLock = new ReentrantLock();
    private volatile Status status = Status.Unstarted;

    /**
     * Creates a new PuServer
     */
    public PuServer() {
        this(DEFAULT_SCAN_DELAY_MS);
    }

    /**
     * Creates a new PuServer with the given scanDelayMs.
     *
     * @param scanDelayMs the period to wait between scans.
     * @throws IllegalArgumentException if scanDelayMs smaller than zero.
     */
    public PuServer(long scanDelayMs) {
        if (scanDelayMs < 0) {
            throw new IllegalArgumentException("scanDelayMs can't be smaller or equal than zero, scanDelayMs was " + scanDelayMs);
        }
        scheduler = new ScheduledThreadPoolExecutor(1);
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(true);
        puContainer = PuContainer.INSTANCE;
        puMonitor = new PuMonitor(puContainer);
    }

    /**
     * Starts the PuServer.
     * <p/>
     * This call safely can be made if the PuServer already has been started.
     * <p/>
     * This method is threadsafe.
     *
     * @throws IllegalStateException if the PuServer already is shutdown or terminated.
     */
    public void start() {
        logger.log(Level.INFO, "Start");

        stateLock.lock();
        try {
            switch (status) {
                case Unstarted:
                    scheduler.scheduleAtFixedRate(new ScanTask(), 0, 5, TimeUnit.SECONDS);

                    logger.log(Level.FINE, "Started");
                    status = Status.Running;
                    break;
                case Running:
                    logger.log(Level.FINE, "Start call is ignored, PuServer is already running");
                    break;
                case Terminating:
                    throw new IllegalStateException("Can't start an already shutdown PuServer");
                case Terminated:
                    throw new IllegalStateException("Can't start an already terminated PuServer");
                default:
                    throw new IllegalStateException("Unrecognized states: " + status);
            }
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Shuts down this PuServer.
     * <p/>
     * This call is threadsafe.
     * <p/>
     * This call can safely be made if this PuServer already is shutdown or terminated.
     * <p/>
     * This call gives no guarantee that the PuServer has terminated after this call completes. To wait for termination,
     * call the {@link #awaitTermination()} or {@link #awaitTermination(long, java.util.concurrent.TimeUnit)}.
     */
    public void shutdown() {
        logger.log(Level.INFO, "Shutdown");

        stateLock.lock();
        try {
            switch (status) {
                case Unstarted:
                    logger.log(Level.FINE, "PuServer not started yet, so will be immediately terminated");
                    status = Status.Terminated;
                    break;
                case Running:
                    //TODO: LOGGING
                    status = Status.Terminating;
                    scheduler.shutdown();
                    break;
                case Terminating:
                    logger.log(Level.FINE, "Shutdown call ignored, PuServer already terminating");
                    break;
                case Terminated:
                    logger.log(Level.FINE, "Shutdown call ignored, PuServer already terminated");
                    break;
                default:
                    throw new IllegalStateException("Unrecognized states: " + status);
            }
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Checks if this PuServer is shutdown. It could be that this PuServer is still terminating and has not
     * yet fully terminated.
     *
     * @return true if shutdown, false otherwise.
     */
    public boolean isShutdown() {
        Status s = status;
        return s == Status.Terminating || s == Status.Terminated;
    }

    /**
     * Checks if this PuServer is terminating, but not yet terminated.
     *
     * @return true if terminating, false otherwise.
     */
    //done
    public boolean isTerminating() {
        return status == Status.Terminating;
    }

    /**
     * Checks if this PuServer is terminated (so fully shutdown).
     *
     * @return true if terminated, false otherwise.
     */
    public boolean isTerminated() {
        return status == Status.Terminated;
    }


    protected Status getStatus() {
        return status;
    }

    /**
     * Blocks until this PuServer has fully terminated after a shutdown
     * request, or the current thread is interrupted, whichever happens first.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    public void awaitTermination() throws InterruptedException {
        scheduler.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    /**
     * Blocks until this PuServer has fully terminated after a shutdown
     * request, or the timeout occurs, or the current thread is
     * interrupted, whichever happens first.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the timeout argument
     * @return <tt>true</tt> if this executor terminated and
     *         <tt>false</tt> if the timeout elapsed before termination
     * @throws InterruptedException if interrupted while waiting
     */
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return scheduler.awaitTermination(timeout, unit);
    }

    private class ScanTask implements Runnable {
        public void run() {
            if (status == Status.Terminating) {
                status = Status.Terminated;
                scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            } else {
                try {
                    puMonitor.scan();
                } catch (RuntimeException e) {
                    logger.log(Level.SEVERE, "Failed to run PuMonitor.scan", e);
                }
            }
        }
    }
}
