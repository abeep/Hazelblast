package com.hazelblast.client.annotations;

import com.hazelblast.client.router.NoOpRouter;
import com.hazelblast.client.router.Router;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation that can be placed on a method to indicate that a call is allowed to execute on any of the
 * nodes. So it doesn't matter which node is going to process that call.
 *
 * @author Peter Veentjer.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface LoadBalanced {

    /**
     * The maximum time to wait for a call to complete.
     *
     * @return the maximum time to wait for a call to complete.
     */
    long timeoutMs() default 60 * 1000;

    /**
     * Optionally interrupts the thread that executed the call on the serverside when a timeout happens.
     *
     * @return if the serverside thread should be interrupted on timeout.
     */
    boolean interruptOnTimeout() default true;

    /**
     * The Router class used to load balance calls.
     *
     * @return the Router class.
     */
    Class<? extends Router> loadBalancer() default NoOpRouter.class;
}
