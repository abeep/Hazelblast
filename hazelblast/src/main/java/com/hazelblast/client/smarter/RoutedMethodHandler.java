package com.hazelblast.client.smarter;

import com.hazelblast.client.exceptions.RemoteMethodTimeoutException;
import com.hazelblast.client.router.Target;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.MemberLeftException;

import java.util.concurrent.*;

import static java.lang.String.format;

public abstract class RoutedMethodHandler extends ProxyMethodHandler {

    public final Object invoke(Object proxy, ProxyMethod proxyMethod, Object[] args) throws Throwable {
        //todo: the problem here is that the partition key is not set on the


        Future future;
        if (proxyMethod.router == null) {
            Callable callable = proxyProvider.remoteMethodInvocationFactory.create(
                    proxyProvider.sliceName,
                    proxyMethod.method.getDeclaringClass().getSimpleName(),
                    proxyMethod.method.getName(),
                    args,
                    proxyMethod.argTypes,
                    Long.MIN_VALUE);

            future = executor.submit(callable);
        } else {
            Target target = proxyMethod.router.getTarget(proxyMethod.method, args);

            if (target.getMember() == null) {
                throw new MemberLeftException();
            }

            Callable callable = proxyProvider.remoteMethodInvocationFactory.create(
                    proxyProvider.sliceName,
                    proxyMethod.method.getDeclaringClass().getSimpleName(),
                    proxyMethod.method.getName(),
                    args,
                    proxyMethod.argTypes,
                    target.getPartitionId());
            future = executor.submit(new DistributedTask(callable, target.getMember()));
        }

        try {
            return future.get(proxyMethod.timeoutMs, TimeUnit.MILLISECONDS);
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
            if (proxyMethod.interruptOnTimeout) {
                future.cancel(true);
            }
            throw new RemoteMethodTimeoutException(
                    format("method '%s' failed to complete in the %s ms",
                            proxyMethod.method.toString(), proxyMethod.timeoutMs), e);
        }
    }
}
