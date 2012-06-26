package com.hazelblast.client.basic;

import com.hazelblast.client.exceptions.DistributedMethodTimeoutException;
import com.hazelblast.client.router.Router;
import com.hazelblast.client.router.Target;
import com.hazelblast.server.exceptions.NoMemberAvailableException;
import com.hazelblast.server.exceptions.PartitionMovedException;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.logging.ILogger;

import java.lang.reflect.Method;
import java.util.concurrent.*;
import java.util.logging.Level;

import static java.lang.String.format;


/**
 * A {@link MethodInvocationHandler} specially made for dealing with routed calls, so calls that make use of the
 * {@link Router}. A partitioned calls routs on partitions and a loadbalanced call routes based on load, so they
 * both share this routing aspects. This class contains the logic for executing loadbalanced and partitioned calls.
 *
 * @author Peter Veentjer.
 */
public abstract class RoutedMethodInvocationHandlerFactory extends MethodInvocationHandlerFactory {

    public class RoutedMethodInvocationHandler implements MethodInvocationHandler {
        private final Method method;
        private final String[] argTypes;
        private final long timeoutNs;
        private final Router router;
        private final boolean interruptOnTimeout;
        private final ILogger logger;

        public RoutedMethodInvocationHandler(Method method,
                                             long timeoutMs,
                                             boolean interruptOnTimeout,
                                             Router router) {
            this.logger = hazelcastInstance.getLoggingService().getLogger(RoutedMethodInvocationHandler.class.getName());
            this.method = method;
            if (timeoutMs == Long.MAX_VALUE) {
                this.timeoutNs = Long.MAX_VALUE;
            } else {
                this.timeoutNs = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            }
            this.router = router;
            this.interruptOnTimeout = interruptOnTimeout;

            Class[] parameterTypes = method.getParameterTypes();
            this.argTypes = new String[parameterTypes.length];
            for (int k = 0; k < argTypes.length; k++) {
                argTypes[k] = parameterTypes[k].getName();
            }
        }

        public Object invoke(Object proxy, Object[] args) throws Throwable {
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, "Starting " + method);
            }

            long spendNs = 0;
            for (; ; ) {
                long startTimeNs = System.nanoTime();

                Future future = null;
                try {
                    if (spendNs > timeoutNs) {
                        throw new TimeoutException();
                    }

                    Object result;
                    try {
                        if (router == null) {
                            //if no router is available, we'll let the executor decide if it wants to apply load balancing
                            Callable callable = proxyProvider.distributedMethodInvocationFactory.create(
                                    proxyProvider.sliceName,
                                    method.getDeclaringClass().getSimpleName(),
                                    method.getName(),
                                    args,
                                    argTypes,
                                    -1);

                            future = executor.submit(callable);
                        } else {
                            //a router was found, so we'll use the result of this router to figure out to which machine
                            //the task is send.

                            Target target = router.getTarget(method, args);

                            if (target.getMember() == null) {
                                throw new MemberLeftException();
                            }

                            final Callable callable = proxyProvider.distributedMethodInvocationFactory.create(
                                    proxyProvider.sliceName,
                                    method.getDeclaringClass().getSimpleName(),
                                    method.getName(),
                                    args,
                                    argTypes,
                                    target.getPartitionId());

                            if (target.getMember().equals(hazelcastInstance.getCluster().getLocalMember())) {
                                if (callable instanceof HazelcastInstanceAware) {
                                    ((HazelcastInstanceAware) callable).setHazelcastInstance(hazelcastInstance);
                                }
                                future = new CallerRunsFuture(callable);
                            } else {
                                future = executor.submit(new DistributedTask(callable, target.getMember()));
                            }
                        }

                        if (timeoutNs == Long.MAX_VALUE) {
                            result = future.get();
                        } else {
                            result = future.get(timeoutNs - spendNs, TimeUnit.NANOSECONDS);
                        }
                    } finally {
                        spendNs += System.nanoTime() - startTimeNs;
                    }

                    if (logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, format("Completed '%s' in %s ms", method, TimeUnit.NANOSECONDS.toMillis(spendNs)));
                    }

                    return result;
                } catch (TimeoutException e) {
                    if (future != null && interruptOnTimeout) {
                        future.cancel(true);
                    }
                    throw new DistributedMethodTimeoutException(
                            format("Method '%s' failed to complete in %s ms", method.toString(), TimeUnit.NANOSECONDS.toMillis(timeoutNs)), e);
                } catch (Exception e) {
                    if (isWorthRetrying(e)) {
                        spendNs = sleep(spendNs, timeoutNs);
                    } else {
                        Throwable cause = e;
                        if (e instanceof ExecutionException) {
                            cause = e.getCause();
                            StackTraceElement[] clientSideStackTrace = Thread.currentThread().getStackTrace();
                            fixStackTrace(cause, clientSideStackTrace);
                        }
                        throw cause;
                    }
                }
            }
        }

        private boolean isWorthRetrying(Throwable e) {
            if (e instanceof MemberLeftException) {
                return true;
            }

            if (e instanceof ExecutionException) {
                if (e.getCause() == null) {
                    return true;
                } else {
                    e = e.getCause();
                }
            }

            if (e instanceof PartitionMovedException) {
                return true;
            }

            if (e instanceof NoMemberAvailableException) {
                return true;
            }

            return false;
        }

        private long sleep(long spendNs, long timeoutNs) throws InterruptedException {
            long sleepPeriodNs = TimeUnit.MILLISECONDS.toNanos(100);
            if (timeoutNs != Long.MAX_VALUE) {
                if (sleepPeriodNs > timeoutNs - spendNs) {
                    sleepPeriodNs = timeoutNs - spendNs;
                }
            }

            if (sleepPeriodNs < 0) {
                return spendNs;
            }

            int ns = (int) (sleepPeriodNs % (1000 * 1000));
            long ms = sleepPeriodNs / (1000 * 1000);
            Thread.sleep(ms, ns);
            return spendNs + sleepPeriodNs;
        }

        private class CallerRunsFuture implements Future {
            private final Callable callable;

            public CallerRunsFuture(Callable callable) {
                this.callable = callable;
            }

            public boolean cancel(boolean mayInterruptIfRunning) {
                throw new UnsupportedOperationException();
            }

            public boolean isCancelled() {
                throw new UnsupportedOperationException();
            }

            public boolean isDone() {
                throw new UnsupportedOperationException();
            }

            public Object get() throws InterruptedException, ExecutionException {
                try {
                    return callable.call();
                } catch (Exception e) {
                    throw new ExecutionException(e);
                }
            }

            public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                try {
                    return callable.call();
                } catch (Exception e) {
                    throw new ExecutionException(e);
                }
            }
        }
    }
}
