package com.hazelblast.server;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import org.apache.commons.cli.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import static com.hazelblast.utils.Arguments.notNull;
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
    public static final int DEFAULT_SCAN_DELAY_MS = 1000;
    public static final String DEFAULT_PU_NAME = "default";

    private final ILogger logger;
    private static final ConcurrentMap<Key, ServiceContextServer> serverMap = new ConcurrentHashMap<Key, ServiceContextServer>();

    public static void main(String[] args) {
        Options options = buildOptions();
        CommandLineParser parser = new BasicParser();
        CommandLine commandLine = buildCommandLine(args, options, parser);

        if (commandLine.hasOption("version")) {
            System.out.println("ServiceContextServer version is 0.3-SNAPSHOT");
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

        HazelcastInstance hazelcastInstance = Hazelcast.getDefaultInstance();
        ServiceContextServer main = new ServiceContextServer(buildServiceContext(serviceContextFactory), serviceContextName, scanDelayMs, hazelcastInstance);
        main.start();
    }

    private static CommandLine buildCommandLine(String[] args, Options options, CommandLineParser parser) {
        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println("Parsing failed.  Reason: " + e.getMessage());
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

    /**
     * Gets a ServiceContext with the given name.
     *
     * @param name the name of the ServiceContext.
     * @return the found ServiceContext.
     * @throws NullPointerException  if name is null.
     * @throws IllegalStateException if no ServiceContext with the given name is found.
     */
    public static ServiceContextContainer getContainer(HazelcastInstance hazelcastInstance, String name) {
        notNull("hazelcastInstance", hazelcastInstance);
        notNull("name", name);

        Key key = new Key(hazelcastInstance, name);
        ServiceContextServer server = serverMap.get(key);

        //TODO: Improve exception, also the hazelcastInstance should be part of exception
        if (server == null) {
            throw new IllegalStateException(format("No container found for service context %s@%s on member [%s], available serviceContext's %s",
                    name,hazelcastInstance.getName(), Hazelcast.getCluster().getLocalMember(), serverMap.keySet()));
        }

        return server.container;
    }

    private static ServiceContext buildServiceContext(String factoryName) {
        System.out.printf("Creating ServiceContext using factory [%s]\n", factoryName);

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

    /**
     * Executes a method.
     * <p/>
     * This is the call that is executed by ProxyProvider once the task is deserialized and executed on the target machine.
     *
     * @param serviceContextName
     * @param serviceName
     * @param methodName
     * @param args
     * @return
     * @throws Exception
     * @throws IllegalArgumentException if serviceContextName is not pointing to a an existing ServiceContext.
     * @throws NullPointerException     if serviceContextName
     */
    public static Object executeMethod(HazelcastInstance hazelcastInstance,
                                       String serviceContextName, String serviceName, String methodName,
                                       String[] argTypes, Object[] args, Object partitionKey) throws Throwable {
        notNull("hazelcastInstance", hazelcastInstance);
        notNull("serviceContextName", serviceContextName);
        notNull("serviceName", serviceName);
        notNull("methodName", methodName);

        //The first thing that needs to be checked, is if the partition that was expected to be here when the call
        //was send to this machine, is still there. If it isn't, some kind of exception should be thrown, this exception
        //should be caught by the proxy and the method call should be retried, now hoping that

        ServiceContextContainer container = getContainer(hazelcastInstance, serviceContextName);
        return container.executeMethod(serviceName, methodName, argTypes, args, partitionKey);
    }

    protected enum Status {Unstarted, Running, Terminating, Terminated}

    private final String serviceContextName;
    private final HazelcastInstance hazelcastInstance;
    private final ServiceContextContainer container;
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
        this(serviceContext, serviceContextName, DEFAULT_SCAN_DELAY_MS, Hazelcast.getDefaultInstance());
    }

    /**
     * Creates a ServiceContextServer.
     *
     * @param serviceContext     the ServiceContext that is hosted by this ServiceContextServer.
     * @param serviceContextName the name of the serviceContext.
     * @param scanDelayMs        the delay between partition change checks.
     * @throws NullPointerException     if serviceContext or serviceContextName is null.
     * @throws IllegalArgumentException if scanDelayMs smaller than zero.
     */
    public ServiceContextServer(ServiceContext serviceContext, final String serviceContextName, long scanDelayMs, HazelcastInstance hazelcastInstance) {
        notNull("serviceContext", serviceContext);
        this.serviceContextName = notNull("serviceContextName", serviceContextName);
        this.hazelcastInstance = notNull("hazelcastInstance", hazelcastInstance);
        this.logger = hazelcastInstance.getLoggingService().getLogger(ServiceContextServer.class.getName());

        if (scanDelayMs < 0) {
            throw new IllegalArgumentException(format("scanDelayMs can't be smaller or equal than zero, scanDelayMs was [%s]", scanDelayMs));
        }

        this.scanDelayMs = scanDelayMs;
        this.scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(true);
        this.container = new ServiceContextContainer(serviceContext, serviceContextName, hazelcastInstance);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            {
                setName("ServiceContextServer-" + serviceContextName + "-shutdownHook-thread");
            }

            public void run() {
                if (logger.isLoggable(Level.INFO)) {
                    logger.log(Level.INFO, format("[%s] Shutdown hook is shutting down ServiceContextServer", serviceContextName));
                }
                shutdown();

                if (logger.isLoggable(Level.INFO)) {
                    logger.log(Level.INFO, format("[%s] Shutdown hook is awaiting termination", serviceContextName));
                }
                try {
                    awaitTermination();
                } catch (InterruptedException e) {
                }

                if (logger.isLoggable(Level.INFO)) {
                    logger.log(Level.INFO, format("[%s] Shutdown hook is finished", serviceContextName));
                }
            }
        });
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
                    container.start();

                    if (serverMap.putIfAbsent(new Key(hazelcastInstance, serviceContextName), this) != null) {
                        shutdown();
                        throw new IllegalStateException(
                                format("ServiceContextServer with name [%s] can't be started, there already is another " +
                                        "ServiceContextServer registered under the same name", serviceContextName));
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
                    if (logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, format("[%s] ServiceContextServer not started yet, so will be immediately terminated", serviceContextName));
                    }
                    scheduler.shutdown();
                    status = Status.Terminated;
                    break;
                case Running:
                    serverMap.remove(new Key(hazelcastInstance, serviceContextName), this);
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
                container.stop();
                status = Status.Terminated;
                scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            } else {
                try {
                    container.scanForPartitionChanges();
                } catch (Throwable e) {
                    logger.log(Level.SEVERE, "Failed to run ServiceContextContainer.scanForPartitionChanges", e);
                }
            }
        }
    }

    private static class Key {
        private final HazelcastInstance hazelcastInstance;
        private final String name;

        private Key(HazelcastInstance hazelcastInstance, String name) {
            this.hazelcastInstance = hazelcastInstance;
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Key key = (Key) o;

            if (!hazelcastInstance.getName().equals(key.hazelcastInstance.getName())) return false;
            if (!name.equals(key.name)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = hazelcastInstance.hashCode();
            result = 31 * result + name.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return name+"@"+hazelcastInstance.getName();
        }
    }
}
