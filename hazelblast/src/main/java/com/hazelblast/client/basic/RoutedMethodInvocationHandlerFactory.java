package com.hazelblast.client.basic;

import com.hazelblast.client.exceptions.DistributedMethodTimeoutException;
import com.hazelblast.client.router.Router;
import com.hazelblast.client.router.Target;
import com.hazelblast.server.exceptions.NoMemberAvailableException;
import com.hazelblast.server.exceptions.PartitionMovedException;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.logging.ILogger;

import java.lang.reflect.Method;
import java.util.concurrent.*;
import java.util.logging.Level;

import static java.lang.String.format;

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
                            Callable callable = proxyProvider.distributedMethodInvocationFactory.create(
                                    proxyProvider.sliceName,
                                    method.getDeclaringClass().getSimpleName(),
                                    method.getName(),
                                    args,
                                    argTypes,
                                    -1);

                            future = executor.submit(callable);
                        } else {
                            Target target = router.getTarget(method, args);

                            if (target.getMember() == null) {
                                throw new MemberLeftException();
                            }

                            Callable callable = proxyProvider.distributedMethodInvocationFactory.create(
                                    proxyProvider.sliceName,
                                    method.getDeclaringClass().getSimpleName(),
                                    method.getName(),
                                    args,
                                    argTypes,
                                    target.getPartitionId());
                            future = executor.submit(new DistributedTask(callable, target.getMember()));
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
    }
}
