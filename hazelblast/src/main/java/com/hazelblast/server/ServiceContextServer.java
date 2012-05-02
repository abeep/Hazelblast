package com.hazelblast.server;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.apache.commons.cli.*;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import static java.lang.String.format;

/**
 * The ServiceContextServer is responsible for hosting the {@link ServiceContextContainer} and can either run
 * standalone, or can be embedded in an existing java application.
 * <p/>
 * If the ServiceContextServer is embedded, Multiple ServiceContextServers can be run in parallel.
 * <p/>
 * As soon as a ServiceContextServer is started, it automatically registers itself in a global registry (contained in
 * this ServiceContextServer) so that a client can look up the ServiceContextServer when a remote call is executed. When
 * the ServiceContextServer stops, the ServiceContextServer will automatically be unregistered.
 *
 * @author Peter Veentjer.
 */
public final class ServiceContextServer {
    public static final int DEFAULT_SCAN_DELAY_MS = 5000;
    public static final String DEFAULT_PU_NAME = "default";

    private static final ILogger logger = Logger.getLogger(ServiceContextServer.class.getName());
    private static final ConcurrentMap<String, ServiceContextServer> serviceContextMap = new ConcurrentHashMap<String, ServiceContextServer>();

    public static void main(String[] args) {
        Options options = buildOptions();
        CommandLineParser parser = new BasicParser();
        CommandLine commandLine = buildCommandLine(args, options, parser);

        if (commandLine.hasOption("version")) {
            logger.log(Level.INFO, "ServiceContextServer version is 0.1-SNAPSHOT");
            System.exit(0);
        }

        if (commandLine.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("ant", options);
            System.exit(0);
        }

        String serviceContextName = commandLine.getOptionValue("serviceContextName", DEFAULT_PU_NAME);
        String serviceContextFactory = commandLine.getOptionValue("serviceContextFactory");
        long scanDelayMs = Long.parseLong(commandLine.getOptionValue("scanDelay", "" + DEFAULT_SCAN_DELAY_MS));

        ServiceContextServer main = new ServiceContextServer(buildPu(serviceContextFactory), serviceContextName, scanDelayMs);
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
        Option serviceContextName = OptionBuilder.withArgName("serviceContextName")
                .hasArg()
                .withDescription("The name of the service context")
                .withType(String.class)
                .create("serviceContextFactory");

        Option serviceContextFactory = OptionBuilder.withArgName("serviceContextFactory")
                .hasArg()
                .isRequired(true)
                .withType(String.class)
                .withDescription("The class of the com.hazelblast.api.ServiceContextFactory instance that creates the ServiceContext")
                .create("serviceContextFactory");

        Option scanDelay = OptionBuilder.withArgName("scanDelay")
                .hasArg()
                .withDescription("The delay in milliseconds between checking if the partitions have moved")
                .withType(Long.class)
                .create("scanDelay");

        Option help = new Option("help", "Print this message");
        Option version = new Option("version", "Print the version information and exit");

        Options options = new Options();
        options.addOption(serviceContextName);
        options.addOption(serviceContextFactory);
        options.addOption(scanDelay);
        options.addOption(help);
        options.addOption(version);
        return options;
    }

    public static ServiceContext getProcessingUnit(String name) {
        if (name == null) {
            throw new NullPointerException("name can't be null");
        }

        ServiceContextServer server = serviceContextMap.get(name);
        if (server == null) {
            throw new IllegalStateException(format("No serviceContext found with name [%s] on member [%s], available serviceContext's %s",
                    name, Hazelcast.getCluster().getLocalMember(), serviceContextMap.keySet()));
        }

        return server.serviceContextContainer.getServiceContext();
    }

    private static ServiceContext buildPu(String factoryName) {
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, format("Creating ServiceContext using puFactory [%s]", factoryName));
        }

        ClassLoader classLoader = ServiceContextServer.class.getClassLoader();
        try {
            Class<ServiceContextFactory> factoryClazz = (Class<ServiceContextFactory>) classLoader.loadClass(factoryName);
            ServiceContextFactory serviceContextFactory = factoryClazz.newInstance();
            return serviceContextFactory.create();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(format("Failed to create ServiceContext using puFactory.class [%s] ", factoryName), e);
        } catch (InstantiationException e) {
            throw new RuntimeException(format("Failed to create ServiceContext using puFactor.class [%s]", factoryName), e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(format("Failed to create ServiceContext using puFactor.class [%s]", factoryName), e);
        }
    }

    public static Object executeMethod(String serviceContextName, String serviceName, String methodName, Object[] args) throws Exception{
        ServiceContext serviceContext = ServiceContextServer.getProcessingUnit(serviceContextName);

        Object service = serviceContext.getService(serviceName);

        Class[] argTypes = new Class[args.length];
        for (int k = 0; k < argTypes.length; k++) {
            argTypes[k] = args[k].getClass();
        }

        Method method = service.getClass().getMethod(methodName, argTypes);
        return method.invoke(service, args);
    }

    protected enum Status {Unstarted, Running, Terminating, Terminated}

    private final String serviceContextName;
    private final PartitionMonitor partitionMonitor;
    private final ServiceContextContainer serviceContextContainer;
    private final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
    private final Lock stateLock = new ReentrantLock();
    private final long scanDelayMs;
    private volatile Status status = Status.Unstarted;

    /**
     * Creates a new ServiceContextServer.
     *
     * @param serviceContext     the serviceContext this ServiceContextServer 'contains'.
     * @param serviceContextName the name of the serviceContext.
     * @throws NullPointerException if serviceContext or serviceContextName is null.
     */
    public ServiceContextServer(ServiceContext serviceContext, String serviceContextName) {
        this(serviceContext, serviceContextName, DEFAULT_SCAN_DELAY_MS);
    }

    /**
     * Creates a ServiceContextServer.
     *
     * @param serviceContext          the ServiceContext that is hosted by this ServiceContextServer.
     * @param serviceContextName      the name of the serviceContext.
     * @param scanDelayMs the delay between partition change checks.
     * @throws NullPointerException     if serviceContext or serviceContextName is null.
     * @throws IllegalArgumentException if scanDelayMs smaller than zero.
     */
    public ServiceContextServer(ServiceContext serviceContext, String serviceContextName, long scanDelayMs) {
        if (serviceContext == null) {
            throw new NullPointerException("serviceContext can't be null");
        }

        if (scanDelayMs < 0) {
            throw new IllegalArgumentException(format("scanDelayMs can't be smaller or equal than zero, scanDelayMs was [%s]", scanDelayMs));
        }

        if (serviceContextName == null) {
            throw new NullPointerException("serviceContextName can't be null");
        }

        this.serviceContextName = serviceContextName;
        this.scanDelayMs = scanDelayMs;
        this.scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(true);
        this.serviceContextContainer = new ServiceContextContainer(serviceContext, serviceContextName);
        this.partitionMonitor = new PartitionMonitor(serviceContextContainer);
    }

    /**
     * Starts the ServiceContextServer.
     * <p/>
     * This call safely can be made if the ServiceContextServer already has been started.
     * <p/>
     * This method is thread safe.
     *
     * @throws IllegalStateException if the ServiceContextServer already is shutdown or terminated or if another processing unit with the same name
     *                               has been started.
     */
    public void start() {
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, format("[%s] Start", serviceContextName));
        }

        stateLock.lock();
        try {
            switch (status) {
                case Unstarted:
                    status = Status.Running;
                    serviceContextContainer.onStart();

                    if (serviceContextMap.putIfAbsent(serviceContextName, this) != null) {
                        shutdown();
                        throw new IllegalStateException(
                                format("ServiceContextServer with name [%s] can't be started, there is another ServiceContextServer registered under the same name", serviceContextName));
                    }

                    scheduler.scheduleAtFixedRate(new ScanTask(), 0, scanDelayMs, TimeUnit.MILLISECONDS);
                    if (logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, format("[%s] Started", serviceContextName));
                    }

                    break;
                case Running:
                    if (logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, format("[%s] Start call is ignored, ServiceContextServer is already running", serviceContextName));
                    }
                    break;
                case Terminating:
                    throw new IllegalStateException("Can't start an already shutdown ServiceContextServer");
                case Terminated:
                    throw new IllegalStateException("Can't start an already terminated ServiceContextServer");
                default:
                    throw new IllegalStateException("Unrecognized states: " + status);
            }
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Shuts down this ServiceContextServer.
     * <p/>
     * This call is thread safe.
     * <p/>
     * This call can safely be made if this ServiceContextServer already is shutdown or terminated.
     * <p/>
     * This call gives no guarantee that the ServiceContextServer has terminated after this call completes. To wait for termination,
     * call the {@link #awaitTermination()} or {@link #awaitTermination(long, java.util.concurrent.TimeUnit)}.
     */
    public void shutdown() {
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, format("[%s] Shutdown", serviceContextName));
        }

        stateLock.lock();
        try {
            switch (status) {
                case Unstarted:
                    serviceContextMap.remove(serviceContextName, this);
                    serviceContextContainer.onStop();
                    if (logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, format("[%s] ServiceContextServer not started yet, so will be immediately terminated", serviceContextName));
                    }
                    status = Status.Terminated;
                    break;
                case Running:
                    serviceContextMap.remove(serviceContextName, this);
                    if (logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, format("[%s] ServiceContextServer is running, and will now be terminating", serviceContextName));
                    }
                    status = Status.Terminating;
                    scheduler.shutdown();
                    break;
                case Terminating:
                    if (logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, format("[%s] Shutdown call ignored, ServiceContextServer already terminating", serviceContextName));
                    }
                    break;
                case Terminated:
                    if (logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, format("[%s] Shutdown call ignored, ServiceContextServer already terminated", serviceContextName));
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
     * Checks if this ServiceContextServer is shutdown. It could be that this ServiceContextServer is still terminating and has not
     * yet fully terminated.
     *
     * @return true if shutdown, false otherwise.
     */
    public boolean isShutdown() {
        Status s = status;
        return s == Status.Terminating || s == Status.Terminated;
    }

    /**
     * Checks if this ServiceContextServer is terminating, but not yet terminated.
     *
     * @return <tt>true</tt> if terminating, <tt>false</tt> otherwise.
     */
    //done
    public boolean isTerminating() {
        return status == Status.Terminating;
    }

    /**
     * Checks if this ServiceContextServer is terminated (so fully shutdown).
     *
     * @return <tt>true</tt> if terminated, <tt>false</tt> otherwise.
     */
    public boolean isTerminated() {
        return status == Status.Terminated;
    }

    /**
     * Returns the Status of this ServiceContextServer. This method is only here for testing purposes.
     *
     * @return the status.
     */
    protected Status getStatus() {
        return status;
    }

    /**
     * Blocks until this ServiceContextServer has fully terminated after a shutdown
     * request, or the current thread is interrupted, whichever happens first.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    public void awaitTermination() throws InterruptedException {
        scheduler.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    /**
     * Blocks until this ServiceContextServer has fully terminated after a shutdown
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
                    partitionMonitor.scan();
                } catch (RuntimeException e) {
                    logger.log(Level.SEVERE, "Failed to run PartitionMonitor.scan", e);
                }
            }
        }
    }
}
