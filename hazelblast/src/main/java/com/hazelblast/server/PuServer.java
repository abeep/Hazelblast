package com.hazelblast.server;

import com.hazelblast.api.ProcessingUnit;
import com.hazelblast.api.PuFactory;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.apache.commons.cli.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import static java.lang.String.format;

/**
 * The PuServer is responsible for hosting the {@link PuContainer} and can either run
 * standalone, or can be embedded in an existing java application.
 * <p/>
 * If the PuServer is embedded, Multiple PuServers can be run in parallel.
 * <p/>
 * As soon as a PuServer is started, it automatically registers itself in a global registry (contained in this PuServer)
 * so that a client can look up the PuServer when a remote call is executed. When the PuServer stops, the PuServer will
 * automatically be unregistered.
 *
 * @author Peter Veentjer.
 */
public final class PuServer {
    public static final int DEFAULT_SCAN_DELAY_MS = 5000;
    public static final String DEFAULT_PU_NAME = "default";

    private static final ILogger logger = Logger.getLogger(PuServer.class.getName());
    private static final ConcurrentMap<String, PuServer> puMap = new ConcurrentHashMap<String, PuServer>();

    public static void main(String[] args) {
        Options options = buildOptions();
        CommandLineParser parser = new BasicParser();
        CommandLine commandLine = buildCommandLine(args, options, parser);

        if (commandLine.hasOption("version")) {
            logger.log(Level.INFO, "PuServer version is 0.1-SNAPSHOT");
            System.exit(0);
        }

        if (commandLine.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("ant", options);
            System.exit(0);
        }

        String puName = commandLine.getOptionValue("puName", DEFAULT_PU_NAME);
        String puFactory = commandLine.getOptionValue("puFactory");
        long scanDelayMs = Long.parseLong(commandLine.getOptionValue("scanDelay", "" + DEFAULT_SCAN_DELAY_MS));

        PuServer main = new PuServer(buildPu(puFactory), puName, scanDelayMs);
        main.start();
    }

    private static CommandLine buildCommandLine(String[] args, Options options, CommandLineParser parser) {
        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            logger.log(Level.SEVERE, "Parsing failed.  Reason: " + e.getMessage());
            System.exit(1);
            //will never be called.
            return null;
        }
    }

    private static Options buildOptions() {
        Option puFile = OptionBuilder.withArgName("puName")
                .hasArg()
                .withDescription("The name of the processing unit")
                .withType(String.class)
                .create("puName");

        Option puFactory = OptionBuilder.withArgName("puFactory")
                .hasArg()
                .isRequired(true)
                .withType(String.class)
                .withDescription("The class of the com.hazelblast.api.PuFactory instance that creates the processing unit")
                .create("puFactory");

        Option scanDelay = OptionBuilder.withArgName("scanDelay")
                .hasArg()
                .withDescription("The delay in milliseconds between checking if the partitions have moved")
                .withType(Long.class)
                .create("scanDelay");

        Option help = new Option("help", "Print this message");
        Option version = new Option("version", "Print the version information and exit");

        Options options = new Options();
        options.addOption(puFile);
        options.addOption(puFactory);
        options.addOption(scanDelay);
        options.addOption(help);
        options.addOption(version);
        return options;
    }

    public static ProcessingUnit getProcessingUnit(String name) {
        if (name == null) {
            throw new NullPointerException("name can't be null");
        }

        PuServer server = puMap.get(name);
        if (server == null) {
            throw new IllegalStateException(format("No pu found with name [%s] on member [%s], available pu's %s",
                    name, Hazelcast.getCluster().getLocalMember(),puMap.keySet()));
        }

        return server.puContainer.getPu();
    }

    private static ProcessingUnit buildPu(String factoryName) {
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, format("Creating ProcessingUnit using puFactory [%s]", factoryName));
        }

        ClassLoader classLoader = PuServer.class.getClassLoader();
        try {
            Class<PuFactory> factoryClazz = (Class<PuFactory>) classLoader.loadClass(factoryName);
            PuFactory puFactory = factoryClazz.newInstance();
            return puFactory.create();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(format("Failed to create ProcessingUnit using puFactory.class [%s] ", factoryName), e);
        } catch (InstantiationException e) {
            throw new RuntimeException(format("Failed to create ProcessingUnit using puFactor.class [%s]", factoryName), e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(format("Failed to create ProcessingUnit using puFactor.class [%s]", factoryName), e);
        }
    }

    protected enum Status {Unstarted, Running, Terminating, Terminated}

    private final String puName;
    private final PuMonitor puMonitor;
    private final PuContainer puContainer;
    private final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
    private final Lock stateLock = new ReentrantLock();
    private final long scanDelayMs;
    private volatile Status status = Status.Unstarted;

    /**
     * Creates a new PuServer.
     *
     * @param pu     the pu this PuServer 'contains'.
     * @param puName the name of the pu.
     * @throws NullPointerException if pu or puName is null.
     */
    public PuServer(ProcessingUnit pu, String puName) {
        this(pu, puName, DEFAULT_SCAN_DELAY_MS);
    }

    /**
     * Creates a PuServer.
     *
     * @param pu          the ProcessingUnit that is hosted by this PuServer.
     * @param puName      the name of the pu.
     * @param scanDelayMs the delay between partition change checks.
     * @throws NullPointerException     if pu or puName is null.
     * @throws IllegalArgumentException if scanDelayMs smaller than zero.
     */
    public PuServer(ProcessingUnit pu, String puName, long scanDelayMs) {
        if (pu == null) {
            throw new NullPointerException("pu can't be null");
        }

        if (scanDelayMs < 0) {
            throw new IllegalArgumentException(format("scanDelayMs can't be smaller or equal than zero, scanDelayMs was [%s]", scanDelayMs));
        }

        if (puName == null) {
            throw new NullPointerException("puName can't be null");
        }

        this.puName = puName;
        this.scanDelayMs = scanDelayMs;
        this.scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(true);
        this.puContainer = new PuContainer(pu,puName);
        this.puMonitor = new PuMonitor(puContainer);
    }

    /**
     * Starts the PuServer.
     * <p/>
     * This call safely can be made if the PuServer already has been started.
     * <p/>
     * This method is thread safe.
     *
     * @throws IllegalStateException if the PuServer already is shutdown or terminated or if another processing unit with the same name
     *                               has been started.
     */
    public void start() {
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, format("[%s] Start", puName));
        }

        stateLock.lock();
        try {
            switch (status) {
                case Unstarted:
                    status = Status.Running;
                    puContainer.onStart();

                    if (puMap.putIfAbsent(puName, this) != null) {
                        shutdown();
                        throw new IllegalStateException(
                                format("PuServer with name [%s] can't be started, there is another PuServer registered under the same name", puName));
                    }

                    scheduler.scheduleAtFixedRate(new ScanTask(), 0, scanDelayMs, TimeUnit.MILLISECONDS);
                    if (logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, format("[%s] Started", puName));
                    }

                    break;
                case Running:
                    if (logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, format("[%s] Start call is ignored, PuServer is already running", puName));
                    }
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
     * This call is thread safe.
     * <p/>
     * This call can safely be made if this PuServer already is shutdown or terminated.
     * <p/>
     * This call gives no guarantee that the PuServer has terminated after this call completes. To wait for termination,
     * call the {@link #awaitTermination()} or {@link #awaitTermination(long, java.util.concurrent.TimeUnit)}.
     */
    public void shutdown() {
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, format("[%s] Shutdown", puName));
        }

        stateLock.lock();
        try {
            switch (status) {
                case Unstarted:
                    puMap.remove(puName, this);
                    puContainer.onStop();
                    if (logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, format("[%s] PuServer not started yet, so will be immediately terminated", puName));
                    }
                    status = Status.Terminated;
                    break;
                case Running:
                    puMap.remove(puName, this);
                    if (logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, format("[%s] PuServer is running, and will now be terminating", puName));
                    }
                    status = Status.Terminating;
                    scheduler.shutdown();
                    break;
                case Terminating:
                    if (logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, format("[%s] Shutdown call ignored, PuServer already terminating", puName));
                    }
                    break;
                case Terminated:
                    if (logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, format("[%s] Shutdown call ignored, PuServer already terminated", puName));
                    }
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
     * @return <tt>true</tt> if terminating, <tt>false</tt> otherwise.
     */
    //done
    public boolean isTerminating() {
        return status == Status.Terminating;
    }

    /**
     * Checks if this PuServer is terminated (so fully shutdown).
     *
     * @return <tt>true</tt> if terminated, <tt>false</tt> otherwise.
     */
    public boolean isTerminated() {
        return status == Status.Terminated;
    }

    /**
     * Returns the Status of this PuServer. This method is only here for testing purposes.
     *
     * @return the status.
     */
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
     * @throws NullPointerException if unit is null
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
