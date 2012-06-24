package com.hazelblast.client.basic;

import com.hazelblast.client.exceptions.RemoteMethodTimeoutException;
import com.hazelblast.client.router.Router;
import com.hazelblast.client.router.Target;
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
        private final long timeoutMs;
        private final Router router;
        private final boolean interruptOnTimeout;
        private final ILogger logger;

        public RoutedMethodInvocationHandler(Method method,
                                             long timeoutMs,
                                             boolean interruptOnTimeout,
                                             Router router) {
            this.method = method;
            this.timeoutMs = timeoutMs;
            this.router = router;
            this.interruptOnTimeout = interruptOnTimeout;

            Class[] parameterTypes = method.getParameterTypes();
            argTypes = new String[parameterTypes.length];
            for (int k = 0; k < argTypes.length; k++) {
                argTypes[k] = parameterTypes[k].getName();
            }

            logger = hazelcastInstance.getLoggingService().getLogger(RoutedMethodInvocationHandler.class.getName());
        }

        public Object invoke(Object proxy, Object[] args) throws Throwable {
            long startTimeNs = 0;
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, "Starting " + method);
                startTimeNs = System.nanoTime();
            }

            //todo: logic is not taking timeout into account.
            while (true) {
                try {
                    Object result = doInvoke(args);

                    if (logger.isLoggable(Level.FINE)) {
                        long delayNs = System.nanoTime() - startTimeNs;
                        logger.log(Level.FINE, "Completed " + method + " in " + (delayNs / 1000) + " microseconds");
                    }

                    return result;
                } catch (PartitionMovedException e) {
                    logger.log(Level.INFO, "Method invocation was send to bad partition, retrying");
                    //we wait some, since it will take some time for this partition to be started on another node.
                    //there is no need to pound the system with useless requests.
                    Thread.sleep(100);
                } catch (MemberLeftException e) {
                    logger.log(Level.INFO, "Method invocation was send to a member that left, retrying");
                    //we wait some, since it will take some time for this partition to be started on another node.
                    //there is no need to pound the system with useless requests.
                    Thread.sleep(100);
                }
            }
        }

        private Object doInvoke(Object[] args) throws Throwable {
            Future future;
            if (router == null) {
                Callable callable = proxyProvider.distributedMethodInvocationFactory.create(
                        proxyProvider.sliceName,
                        method.getDeclaringClass().getSimpleName(),
                        method.getName(),
                        args,
                        argTypes,
                        Long.MIN_VALUE);

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

            try {
                return future.get(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (MemberLeftException e) {
                throw e;
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause == null) {
                    cause = new MemberLeftException();
                } else {
                    StackTraceElement[] clientSideStackTrace = Thread.currentThread().getStackTrace();
                    fixStackTrace(cause, clientSideStackTrace);
                }
                throw cause;
            } catch (TimeoutException e) {
                if (interruptOnTimeout) {
                    future.cancel(true);
                }
                throw new RemoteMethodTimeoutException(
                        format("method '%s' failed to complete in the %s ms",
                                method.toString(), timeoutMs), e);
            }
        }
    }
}
